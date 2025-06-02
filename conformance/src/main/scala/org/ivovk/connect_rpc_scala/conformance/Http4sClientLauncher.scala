package org.ivovk.connect_rpc_scala.conformance

import cats.effect.std.Dispatcher
import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import com.google.protobuf.any.Any
import connectrpc.ErrorDetailsAny
import connectrpc.conformance.v1 as conformance
import connectrpc.conformance.v1.*
import io.grpc.Metadata
import org.http4s.Uri
import org.http4s.ember.client.EmberClientBuilder
import org.ivovk.connect_rpc_scala.conformance.util.{ConformanceHeadersConv, ProtoSerDeser}
import org.ivovk.connect_rpc_scala.connect.ErrorHandling
import org.ivovk.connect_rpc_scala.http4s.ConnectHttp4sClientBuilder
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

/**
 * Flow:
 *
 * TODO: Describe the flow of the conformance client.
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

    val res = for
      dispatcher <- Dispatcher.parallel[IO]
      httpClient <- EmberClientBuilder.default[IO].build

      _ <- fs2.Stream
        .unfoldEval(0) { _ =>
          ProtoSerDeser[IO].read[ClientCompatRequest](System.in).option.map(_.map(_ -> 0))
        }
        .evalMap { (spec: ClientCompatRequest) =>
          val f = for
            _ <- IO(logger.info(s"Received request: $spec")).toResource

            baseUri <- IO.fromEither(Uri.fromString(s"http://${spec.host}:${spec.port}")).toResource

            channel <- ConnectHttp4sClientBuilder(httpClient)
              .withJsonCodecConfigurator(
                // Registering message types in TypeRegistry is required to pass com.google.protobuf.any.Any
                // JSON-serialization conformance tests
                _
                  .registerType[conformance.UnaryRequest]
                  .registerType[conformance.IdempotentUnaryRequest]
              )
              .withRequestTimeout(spec.timeoutMs.map(Duration(_, TimeUnit.MILLISECONDS)))
              .build(baseUri)

            stub = ConformanceServiceFs2GrpcTrailers.stub[IO](dispatcher, channel)

            resp <- testClientCompat(stub, spec).toResource

            _ <- ProtoSerDeser[IO].write(System.out, resp).toResource
          yield ()

          f.use_
        }.compile.resource.drain
    yield logger.info("Conformance client tests finished.")

    res
      .use_
      .onError { case e: Throwable =>
        IO(logger.error("An error occurred during conformance client tests.", e))
      }
  }

  private def testClientCompat(
    stub: ConformanceServiceFs2GrpcTrailers[IO, Metadata],
    spec: ClientCompatRequest,
  ): IO[ClientCompatResponse] = {
    logger.info(">>> Running conformance test: {}", spec.testName)

    require(
      spec.service.contains("connectrpc.conformance.v1.ConformanceService"),
      s"Invalid service name: ${spec.service}. Expected 'connectrpc.conformance.v1.ConformanceService'.",
    )

    spec.method match {
      case Some("Unary") =>
        val req             = spec.requestMessages.head.unpack[UnaryRequest]
        val requestMetadata = ConformanceHeadersConv.toMetadata(spec.requestHeaders)

        logger.info("Decoded Request: {}", req)
        logger.info("Decoded Request metadata: {}", requestMetadata)

        stub.unary(req, requestMetadata)
          .map { (resp, trailers) =>
            logger.info("<<< Conformance test completed: {}", spec.testName)

            ClientCompatResponse.Result.Response(
              ClientResponseResult(
                responseHeaders = ConformanceHeadersConv.toHeaderSeq(trailers),
                payloads = resp.payload.toSeq,
                responseTrailers = ConformanceHeadersConv.toTrailingHeaderSeq(trailers),
              )
            )
          }
          .handleError { (t: Throwable) =>
            val errorDetails = ErrorHandling.extractDetails(t)
            logger.info(s"Error during the conformance test: ${spec.testName}. Error: $errorDetails")

            ClientCompatResponse.Result.Response(
              ClientResponseResult(
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
                responseTrailers = ConformanceHeadersConv.toTrailingHeaderSeq(errorDetails.metadata),
              )
            )
          }
          .map(result => ClientCompatResponse(spec.testName, result))
      case Some(other) =>
        ClientCompatResponse(spec.testName)
          .withError(
            ClientErrorResult(
              message = s"Unsupported method: $other. Only 'Unary' is supported in this client."
            )
          )
          .pure[IO]
      case None =>
        ClientCompatResponse(spec.testName)
          .withError(
            ClientErrorResult(
              message = s"Method is not specified in the request."
            )
          )
          .pure[IO]
    }
  }

}
