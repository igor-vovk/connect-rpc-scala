package org.ivovk.connect_rpc_scala.conformance

import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import com.google.protobuf.any.Any
import connectrpc.ErrorDetailsAny
import connectrpc.conformance.v1 as conformance
import connectrpc.conformance.v1.*
import fs2.Stream
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import fs2.io.{stdin, stdout}
import io.grpc.{CallOptions, Channel, MethodDescriptor}
import org.http4s.Uri
import org.http4s.ember.client.EmberClientBuilder
import org.ivovk.connect_rpc_scala.conformance.util.{ConformanceHeadersConv, ProtoCodecs}
import org.ivovk.connect_rpc_scala.connect.ErrorHandling
import org.ivovk.connect_rpc_scala.grpc.ClientCalls
import org.ivovk.connect_rpc_scala.http4s.ConnectHttp4sChannelBuilder
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.slf4j.LoggerFactory
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/**
 * Flow:
 *
 *   - Conformance client reads `ClientCompatRequest` messages from STDIN, sent by the tests runner.
 *   - Each message contains a specification of a conformance test to run.
 *   - For each request, the client connects to the specified host and port, and sends the request to the
 *     specified service and method.
 *   - The client waits for the response and sends `ClientCompatResponse` to STDOUT, which contains the
 *     result.
 *
 * All diagnostics should be written to STDERR.
 *
 * Useful links:
 *
 * [[https://github.com/connectrpc/conformance/blob/main/docs/configuring_and_running_tests.md]]
 */
object Http4sClientLauncher extends IOApp.Simple {

  private val logger = LoggerFactory.getLogger(getClass)

  override def run: IO[Unit] = {
    logger.info("Starting conformance client tests...")

    stdin[IO](2048)
      .through(StreamDecoder.many(ProtoCodecs.decoderFor[ClientCompatRequest]).toPipeByte)
      .evalMap { (spec: ClientCompatRequest) =>
        val channel = for
          baseUri <- IO.fromEither(Uri.fromString(s"http://${spec.host}:${spec.port}")).toResource

          httpClient <- EmberClientBuilder.default[IO]
            .pipeIfDefined(spec.timeoutMs)((b, t) => b.withTimeout(t.millis))
            .build

          channel <- ConnectHttp4sChannelBuilder(httpClient)
            .withJsonCodecConfigurator(
              // Registering message types in TypeRegistry is required to pass com.google.protobuf.any.Any
              // JSON-serialization conformance tests
              _
                .registerType[conformance.UnaryRequest]
                .registerType[conformance.IdempotentUnaryRequest]
                .registerType[conformance.ClientStreamRequest]
            )
            .pipeIf(spec.codec.isProto)(_.enableBinaryFormat())
            .build(baseUri)
        yield channel

        channel.use(ch => runTestCase(ch, spec))
      }
      .through(StreamEncoder.many(ProtoCodecs.encoder).toPipeByte[IO])
      .through(stdout)
      .compile.drain
      .redeem(
        err => logger.error("An error occurred during conformance client tests.", err),
        _ => logger.info("Conformance client tests finished."),
      )

  }

  private def runTestCase(
    channel: Channel,
    spec: ClientCompatRequest,
  ): IO[ClientCompatResponse] = {
    logger.info(">>> Running conformance test: {}", spec.testName)

    require(
      spec.service.contains("connectrpc.conformance.v1.ConformanceService"),
      s"Invalid service name: ${spec.service}.",
    )

    def runUnaryRequest[Req <: Message: Companion, Resp](
      methodDescriptor: MethodDescriptor[Req, Resp]
    )(
      extractPayloads: Resp => Seq[conformance.ConformancePayload]
    ): IO[ClientCompatResponse] = {
      val requests = spec.requestMessages.map(_.unpack[Req])
      val metadata = ConformanceHeadersConv.toMetadata(spec.requestHeaders)

      logger.info(">>> Decoded request(s): {}", requests)
      logger.info(">>> Decoded metadata: {}", metadata)

      val callOptions = CallOptions.DEFAULT
        .pipeIfDefined(spec.timeoutMs)((co, t) => co.withDeadlineAfter(t, TimeUnit.MILLISECONDS))

      ClientCalls
        .clientStreamingCall[IO, Req, Resp](
          channel,
          methodDescriptor,
          callOptions,
          metadata,
          Stream.emits(requests),
        )
        .map { resp =>
          logger.info("<<< Conformance test completed: {}", spec.testName)

          ClientCompatResponse(spec.testName).withResponse(
            ClientResponseResult(
              responseHeaders = ConformanceHeadersConv.toHeaderSeq(resp.headers),
              payloads = extractPayloads(resp.value),
              responseTrailers = ConformanceHeadersConv.toHeaderSeq(resp.trailers),
            )
          )
        }
        .handleError { (t: Throwable) =>
          val errorDetails = ErrorHandling.extractDetails(t)
          logger.info(s"Error during the conformance test: ${spec.testName}. Error: $errorDetails")

          ClientCompatResponse(spec.testName).withResponse(
            ClientResponseResult(
              responseHeaders = ConformanceHeadersConv.toHeaderSeq(errorDetails.headers),
              error = Some(
                conformance.Error(
                  code = conformance.Code.fromValue(errorDetails.error.code.value),
                  message = errorDetails.error.message,
                  details = errorDetails.error.details
                    // TODO: simplify
                    .map(d => Any("type.googleapis.com/" + d.`type`, d.value).unpack[ErrorDetailsAny])
                    .map(d => Any(d.`type`, d.value)),
                )
              ),
              responseTrailers = ConformanceHeadersConv.toHeaderSeq(errorDetails.trailers),
            )
          )
        }
        .start
        .flatMap { fiber =>
          if (spec.getCancel.getAfterCloseSendMs > 0) {
            IO.sleep(spec.getCancel.getAfterCloseSendMs.millis) *>
              IO(logger.info(">>> Cancelling call for test: {}", spec.testName)) *>
              fiber.cancel.as(fiber)
          } else {
            fiber.pure[IO]
          }
        }
        .flatMap(_.joinWith(IO.raiseError(new RuntimeException("fiber has been cancelled"))))
    }

    spec.method match {
      case Some("Unary") =>
        runUnaryRequest(ConformanceServiceGrpc.METHOD_UNARY)(_.payload.toSeq)
      case Some("Unimplemented") =>
        runUnaryRequest(ConformanceServiceGrpc.METHOD_UNIMPLEMENTED)(_ => Seq.empty)
      case Some("ClientStream") =>
        runUnaryRequest(ConformanceServiceGrpc.METHOD_CLIENT_STREAM)(_.payload.toSeq)
      case Some(other) =>
        ClientCompatResponse(spec.testName)
          .withError(ClientErrorResult(s"Unsupported method: $other."))
          .pure[IO]
      case None =>
        ClientCompatResponse(spec.testName)
          .withError(ClientErrorResult("Method is not specified in the request."))
          .pure[IO]
    }
  }

}
