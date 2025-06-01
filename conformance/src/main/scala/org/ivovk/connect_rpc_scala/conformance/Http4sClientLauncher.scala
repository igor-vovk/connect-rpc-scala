package org.ivovk.connect_rpc_scala.conformance

import cats.syntax.all.*
import cats.effect.std.Dispatcher
import cats.effect.{IO, IOApp}
import connectrpc.conformance.v1 as conformance
import connectrpc.conformance.v1.*
import io.grpc.{Metadata, StatusRuntimeException}
import org.http4s.Uri
import org.http4s.ember.client.EmberClientBuilder
import org.ivovk.connect_rpc_scala.conformance.util.{ConformanceHeadersConv, ProtoSerDeser}
import org.ivovk.connect_rpc_scala.connect.StatusCodeMappings.toConnectCode
import org.ivovk.connect_rpc_scala.http4s.ConnectHttp4sClientBuilder
import org.slf4j.LoggerFactory

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
        .evalMap { req =>
          val f = for
            _ <- IO(logger.info(s"Received request: $req")).toResource

            baseUri <- IO.fromEither(Uri.fromString(s"http://${req.host}:${req.port}")).toResource

            channel <- new ConnectHttp4sClientBuilder(httpClient).build(baseUri)
            stub = ConformanceServiceFs2GrpcTrailers.stub[IO](dispatcher, channel)

            resp <- runTest(stub, req).toResource

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

  private def runTest(
    stub: ConformanceServiceFs2GrpcTrailers[IO, Metadata],
    specReq: ClientCompatRequest,
  ): IO[ClientCompatResponse] = {
    logger.info(">>> Running conformance test: {}", specReq.testName)

    require(
      specReq.service.contains("connectrpc.conformance.v1.ConformanceService"),
      s"Invalid service name: ${specReq.service}. Expected 'connectrpc.conformance.v1.ConformanceService'.",
    )

    specReq.method match {
      case Some("Unary") =>
        val req             = UnaryRequest.parseFrom(specReq.requestMessages.head.value.newCodedInput())
        val requestMetadata = ConformanceHeadersConv.toMetadata(specReq.requestHeaders)

        logger.info("Decoded Request: {}", req)
        logger.info("Decoded Request metadata: {}", requestMetadata)

        stub.unary(req, requestMetadata)
          .map { (resp, trailers) =>
            logger.info("<<< Conformance test completed: {}", specReq.testName)

            ClientCompatResponse.Result.Response(
              ClientResponseResult(
                responseHeaders = ConformanceHeadersConv.toHeaderSeq(trailers),
                payloads = resp.payload.toSeq,
                responseTrailers = ConformanceHeadersConv.toTrailingHeaderSeq(trailers),
              )
            )
          }
          .handleError {
            case e: StatusRuntimeException =>
              logger.error("Error during conformance test: {}", specReq.testName, e)

              ClientCompatResponse.Result.Response(
                ClientResponseResult(
                  responseHeaders = ConformanceHeadersConv.toHeaderSeq(e.getTrailers),
                  error = Some(
                    conformance.Error(
                      code = conformance.Code.fromValue(e.getStatus.toConnectCode.value),
                      message = Some(e.getMessage),
                      details = Seq.empty,
                    )
                  ),
                  responseTrailers = ConformanceHeadersConv.toTrailingHeaderSeq(e.getTrailers),
                )
              )
            case e: Throwable =>
              ClientCompatResponse.Result.Error(
                ClientErrorResult(
                  message = e.getMessage
                )
              )
          }
          .map(result => ClientCompatResponse(specReq.testName, result))
      case Some(other) =>
        ClientCompatResponse(specReq.testName)
          .withError(
            ClientErrorResult(
              message = s"Unsupported method: $other. Only 'Unary' is supported in this client."
            )
          )
          .pure[IO]
      case None =>
        ClientCompatResponse(specReq.testName)
          .withError(
            ClientErrorResult(
              message = s"Method is not specified in the request."
            )
          )
          .pure[IO]
    }
  }

}
