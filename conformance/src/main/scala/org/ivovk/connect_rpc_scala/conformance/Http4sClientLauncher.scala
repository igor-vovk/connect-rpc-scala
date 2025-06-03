package org.ivovk.connect_rpc_scala.conformance

import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import com.google.protobuf.any.Any
import connectrpc.ErrorDetailsAny
import connectrpc.conformance.v1 as conformance
import connectrpc.conformance.v1.*
import fs2.Stream
import io.grpc.{CallOptions, Channel, MethodDescriptor}
import org.http4s.Uri
import org.http4s.ember.client.EmberClientBuilder
import org.ivovk.connect_rpc_scala.conformance.util.{ConformanceHeadersConv, ProtoSerDeser}
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

    val protoSerDeser = ProtoSerDeser.systemInOut[IO]

    def readNextSpecFromStdIn: Stream[IO, ClientCompatRequest] =
      Stream
        .eval(protoSerDeser.read[ClientCompatRequest])
        .redeemWith(_ => Stream.empty, v => Stream.emit(v) ++ readNextSpecFromStdIn)

    readNextSpecFromStdIn
      .evalMap { (spec: ClientCompatRequest) =>
        (for
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
            )
            .build(baseUri)

          resp <- runTestCase(channel, spec).toResource

          _ <- protoSerDeser.write(resp).toResource
        yield ()).use_
      }
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

    def doRun[Req <: Message: Companion, Resp](
      methodDescriptor: MethodDescriptor[Req, Resp]
    )(
      extractPayloads: Resp => Seq[conformance.ConformancePayload]
    ): IO[ClientCompatResponse] = {
      val request  = spec.requestMessages.head.unpack[Req]
      val metadata = ConformanceHeadersConv.toMetadata(spec.requestHeaders)

      logger.info(">>> Decoded request: {}", request)
      logger.info(">>> Decoded metadata: {}", metadata)

      val callOptions = CallOptions.DEFAULT
        .pipeIfDefined(spec.timeoutMs)((co, t) => co.withDeadlineAfter(t, TimeUnit.MILLISECONDS))

      val (clientCall, respF) = ClientCalls.asyncUnaryCall2[IO, Req, Resp](
        channel,
        methodDescriptor,
        callOptions,
        metadata,
        request,
      )

      val cancelF = if (spec.getCancel.getAfterCloseSendMs > 0) {
        IO.sleep(spec.getCancel.getAfterCloseSendMs.millis) *> IO(
          clientCall.cancel("Requested by specification", null)
        )
      } else IO.unit

      respF
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
        .parProductL(cancelF)
    }

    spec.method match {
      case Some("Unary") =>
        doRun(ConformanceServiceGrpc.METHOD_UNARY)(_.payload.toSeq)
      case Some("Unimplemented") =>
        doRun(ConformanceServiceGrpc.METHOD_UNIMPLEMENTED)(_ => Seq.empty)
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
