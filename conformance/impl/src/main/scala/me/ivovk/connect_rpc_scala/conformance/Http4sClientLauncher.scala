package me.ivovk.connect_rpc_scala.conformance

import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import com.google.protobuf.any.Any
import connectrpc.conformance.v1 as conformance
import connectrpc.conformance.v1.*
import fs2.io.{stdin, stdout}
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import org.http4s.{Headers, Uri}
import org.http4s.ember.client.EmberClientBuilder
import me.ivovk.connect_rpc_scala.conformance.util.{ConformanceHeadersConv, ScodecProto}
import me.ivovk.connect_rpc_scala.http4s.ConnectHttp4sClientBuilder
import me.ivovk.connect_rpc_scala.*
import org.slf4j.LoggerFactory
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.*

/** Connect conformance client for the RPC methods supported by the library. */
object Http4sClientLauncher extends IOApp.Simple {

  private val logger = LoggerFactory.getLogger(getClass)

  override def run: IO[Unit] = {
    logger.info("Starting conformance client tests...")

    stdin[IO](2048)
      .through(StreamDecoder.many(ScodecProto.decoderFor[ClientCompatRequest]).toPipeByte)
      .evalMap { (spec: ClientCompatRequest) =>
        val resources = for
          baseUri    <- IO.fromEither(Uri.fromString(s"http://${spec.host}:${spec.port}")).toResource
          httpClient <- EmberClientBuilder.default[IO]
            .withTimeout(30.seconds)
            .build
        yield ConnectHttp4sClientBuilder(httpClient)
          .withJsonCodecConfigurator(
            _
              .registerType[conformance.UnaryRequest]
              .registerType[conformance.IdempotentUnaryRequest]
              .registerType[conformance.ServerStreamRequest]
          )
          .withMessageEncoding(if spec.codec.isProto then MessageEncoding.Protobuf else MessageEncoding.Json)
          .build(baseUri)

        resources.use(transport => runTestCase(transport, spec))
      }
      .through(StreamEncoder.many(ScodecProto.encoder).toPipeByte[IO])
      .through(stdout)
      .compile.drain
      .redeem(
        err => logger.error("An error occurred during conformance client tests.", err),
        _ => logger.info("Conformance client tests finished."),
      )
  }

  private def runTestCase(
    transport: ConnectTransport[IO],
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
      val headers  = ConformanceHeadersConv.toHttp4sHeaders(spec.requestHeaders)

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
                ).withCause(other)
            }
            logger.info("Error during conformance test {}: {}", spec.testName, connectError)

            ClientCompatResponse(spec.testName).withResponse(
              ClientResponseResult(
                responseHeaders = ConformanceHeadersConv.toHeaderSeq(connectError.headers),
                error = Some(
                  conformance.Error(
                    code = conformance.Code.fromValue(connectError.code.value),
                    message = Option(connectError.getMessage).filter(_.nonEmpty),
                    details = connectError.details.map(detail =>
                      Any(s"type.googleapis.com/${detail.`type`}", detail.value)
                    ),
                  )
                ),
                responseTrailers = ConformanceHeadersConv.toHeaderSeq(connectError.trailers),
              )
            )
          }
      }
    }

    def runServerStreamingRequest[Req <: Message: Companion, Resp <: Message](
      descriptor: ServerStreamingMethodDescriptor[Req, Resp]
    )(
      extractPayloads: Resp => Seq[conformance.ConformancePayload]
    ): IO[ClientCompatResponse] = {
      val requests = spec.requestMessages.map(_.unpack[Req])
      val headers  = ConformanceHeadersConv.toHttp4sHeaders(spec.requestHeaders)

      if requests.size != 1 then
        ClientCompatResponse(spec.testName)
          .withError(
            ClientErrorResult(s"Server-streaming method requires one request, got ${requests.size}.")
          )
          .pure[IO]
      else
        transport.serverStreaming(
          descriptor,
          requests.head,
          ConnectRequestContext(headers, spec.timeoutMs.map(_.millis)),
        ).use { response =>
          val canceled = ConnectException(connectrpc.Code.Canceled, "Canceled by conformance test")
          val events   = spec.cancel.map(_.cancelTiming) match {
            case Some(ClientCompatRequest.Cancel.CancelTiming.AfterCloseSendMs(delay)) =>
              response.stream.events.timeout(delay.millis).handleErrorWith {
                case _: TimeoutException => fs2.Stream.raiseError[IO](canceled)
                case other               => fs2.Stream.raiseError[IO](other)
              }
            case Some(ClientCompatRequest.Cancel.CancelTiming.AfterNumResponses(count)) =>
              response.stream.events.take(count.toLong) ++ fs2.Stream.raiseError[IO](canceled)
            case _ => response.stream.events
          }

          events.attempt.compile.toList.map { observed =>
            val payloads = observed.collect { case Right(ConnectStream.Event.Message(message)) =>
              extractPayloads(message)
            }.flatten
            val trailers = observed.collectFirst { case Right(ConnectStream.Event.End(values)) =>
              values
            }.getOrElse(Headers.empty)
            val error = observed.collectFirst { case Left(value) => asConnectException(value) }

            ClientCompatResponse(spec.testName).withResponse(
              ClientResponseResult(
                responseHeaders = ConformanceHeadersConv.toHeaderSeq(response.headers),
                payloads = payloads,
                error = error.map(toConformanceError),
                responseTrailers = ConformanceHeadersConv.toHeaderSeq(
                  error.fold(trailers)(_.trailers)
                ),
              )
            )
          }
        }.handleError { error =>
          val connectError = asConnectException(error)
          ClientCompatResponse(spec.testName).withResponse(
            ClientResponseResult(
              responseHeaders = ConformanceHeadersConv.toHeaderSeq(connectError.headers),
              error = Some(toConformanceError(connectError)),
              responseTrailers = ConformanceHeadersConv.toHeaderSeq(connectError.trailers),
            )
          )
        }
    }

    spec.method match {
      case Some("Unary") =>
        runUnaryRequest(ConformanceServiceConnect.METHOD_UNARY)(response => Seq(response.getPayload))
      case Some("IdempotentUnary") =>
        runUnaryRequest(ConformanceServiceConnect.METHOD_IDEMPOTENTUNARY)(response =>
          Seq(response.getPayload)
        )
      case Some("Unimplemented") =>
        runUnaryRequest(ConformanceServiceConnect.METHOD_UNIMPLEMENTED)(_ => Seq.empty)
      case Some("ServerStream") =>
        runServerStreamingRequest(ConformanceServiceConnect.METHOD_SERVERSTREAM)(response =>
          Seq(response.getPayload)
        )
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

  private def asConnectException(error: Throwable): ConnectException =
    error match {
      case value: ConnectException => value
      case other                   =>
        ConnectException(
          connectrpc.Code.Unknown,
          Option(other.getMessage).getOrElse("Unknown error"),
        ).withCause(other)
    }

  private def toConformanceError(error: ConnectException): conformance.Error =
    conformance.Error(
      code = conformance.Code.fromValue(error.code.value),
      message = Option(error.getMessage).filter(_.nonEmpty),
      details = error.details.map(detail => Any(s"type.googleapis.com/${detail.`type`}", detail.value)),
    )
}
