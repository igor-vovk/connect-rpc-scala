package org.ivovk.connect_rpc_scala.conformance

import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import com.google.protobuf.any.Any
import connectrpc.conformance.v1 as conformance
import connectrpc.conformance.v1.*
import fs2.io.{stdin, stdout}
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import org.http4s.Uri
import org.http4s.ember.client.EmberClientBuilder
import org.ivovk.connect_rpc_scala.conformance.util.{ConformanceHeadersConv, ProtoCodecs}
import org.ivovk.connect_rpc_scala.http4s.ConnectHttp4sClientBuilder
import org.ivovk.connect_rpc_scala.service.*
import org.slf4j.LoggerFactory
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import scala.concurrent.duration.*

/** Connect conformance client for the unary methods supported by the library. */
object Http4sClientLauncher extends IOApp.Simple {

  private val logger = LoggerFactory.getLogger(getClass)

  override def run: IO[Unit] = {
    logger.info("Starting conformance client tests...")

    stdin[IO](2048)
      .through(StreamDecoder.many(ProtoCodecs.decoderFor[ClientCompatRequest]).toPipeByte)
      .evalMap { (spec: ClientCompatRequest) =>
        val resources = for
          baseUri    <- IO.fromEither(Uri.fromString(s"http://${spec.host}:${spec.port}")).toResource
          httpClient <- EmberClientBuilder.default[IO]
            .withTimeout(30.seconds)
            .build
        yield {
          val builder = ConnectHttp4sClientBuilder(httpClient).withJsonCodecConfigurator(
            _
              .registerType[conformance.UnaryRequest]
              .registerType[conformance.IdempotentUnaryRequest]
          )
          val transport = if spec.codec.isProto then builder.enableBinaryFormat() else builder
          transport.build(baseUri)
        }

        resources.use(transport => runTestCase(transport, spec))
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
    transport: UnaryTransport[IO],
    spec: ClientCompatRequest,
  ): IO[ClientCompatResponse] = {
    logger.info(">>> Running conformance test: {}", spec.testName)

    require(
      spec.service.contains(ConformanceServiceConnect.serviceName),
      s"Invalid service name: ${spec.service}.",
    )

    def runUnaryRequest[Req <: Message: Companion, Resp <: Message](
      descriptor: UnaryMethodDescriptor[Req, Resp]
    )(
      extractPayloads: Resp => Seq[conformance.ConformancePayload]
    ): IO[ClientCompatResponse] = {
      val requests = spec.requestMessages.map(_.unpack[Req])
      val headers  = ConformanceHeadersConv.toConnectHeaders(spec.requestHeaders)

      if requests.size != 1 then
        ClientCompatResponse(spec.testName)
          .withError(ClientErrorResult(s"Unary method requires one request, got ${requests.size}."))
          .pure[IO]
      else {
        val call = transport.unary(
          descriptor,
          requests.head,
          ConnectRequestContext(headers, spec.timeoutMs.map(_.millis)),
        )
        val cancelAfter = spec.getCancel.getAfterCloseSendMs
        val response    =
          if cancelAfter <= 0 then call
          else
            call.timeoutTo(
              cancelAfter.millis,
              ConnectException(connectrpc.Code.Canceled, "Canceled by conformance test").raiseError,
            )

        response
          .map { result =>
            logger.info("<<< Conformance test completed: {}", spec.testName)
            ClientCompatResponse(spec.testName).withResponse(
              ClientResponseResult(
                responseHeaders = ConformanceHeadersConv.toHeaderSeq(result.headers),
                payloads = extractPayloads(result.message),
                responseTrailers = ConformanceHeadersConv.toHeaderSeq(result.trailers),
              )
            )
          }
          .handleError { error =>
            val connectError = error match {
              case value: ConnectException => value
              case other                   =>
                ConnectException(
                  connectrpc.Code.Unknown,
                  Option(other.getMessage).getOrElse("Unknown error"),
                  cause = Some(other),
                )
            }
            logger.info("Error during conformance test {}: {}", spec.testName, connectError)

            ClientCompatResponse(spec.testName).withResponse(
              ClientResponseResult(
                responseHeaders = ConformanceHeadersConv.toHeaderSeq(connectError.headers),
                error = Some(
                  conformance.Error(
                    code = conformance.Code.fromValue(connectError.code.value),
                    message = Option(connectError.getMessage).filter(_.nonEmpty),
                    details = connectError.details.map(detail => Any(detail.`type`, detail.value)),
                  )
                ),
                responseTrailers = ConformanceHeadersConv.toHeaderSeq(connectError.trailers),
              )
            )
          }
      }
    }

    spec.method match {
      case Some("Unary") =>
        runUnaryRequest(ConformanceServiceConnect.METHOD_UNARY)(_.payload.toSeq)
      case Some("IdempotentUnary") =>
        runUnaryRequest(ConformanceServiceConnect.METHOD_IDEMPOTENTUNARY)(_.payload.toSeq)
      case Some("Unimplemented") =>
        runUnaryRequest(ConformanceServiceConnect.METHOD_UNIMPLEMENTED)(_ => Seq.empty)
      case Some(other) =>
        ClientCompatResponse(spec.testName)
          .withError(ClientErrorResult(s"Unsupported method (streaming is not implemented): $other."))
          .pure[IO]
      case None =>
        ClientCompatResponse(spec.testName)
          .withError(ClientErrorResult("Method is not specified in the request."))
          .pure[IO]
    }
  }
}
