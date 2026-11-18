package me.ivovk.connect_rpc_scala.http4s.client

import cats.effect.Async
import cats.effect.syntax.all.*
import cats.implicits.*
import fs2.Stream
import org.http4s.client.Client
import org.http4s.headers.`User-Agent`
import org.http4s.{ContentCoding, Header, Method, ProductId, Request, Uri}
import me.ivovk.connect_rpc_scala.http.MediaTypes
import me.ivovk.connect_rpc_scala.http.codec.*
import me.ivovk.connect_rpc_scala.http4s.headers.HeaderConverters.*
import me.ivovk.connect_rpc_scala.http4s.headers.`Connect-Timeout-Ms`
import me.ivovk.connect_rpc_scala.service.*
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.nio.charset.StandardCharsets

private[http4s] final class Transport[F[_]: Async](
  httpClient: Client[F],
  codecs: ClientCodecs[F],
  baseUri: Uri,
  userAgent: String,
) extends UnaryTransport[F] {

  override def unary[
    RequestMessage <: GeneratedMessage,
    ResponseMessage <: GeneratedMessage,
  ](
    descriptor: UnaryMethodDescriptor[RequestMessage, ResponseMessage],
    request: RequestMessage,
    ctx: ConnectRequestContext,
  ): F[ConnectResponse[ResponseMessage]] = {
    val entity = codecs.chosen.encode(
      Stream.emit(request),
      EncodeOptions(StandardCharsets.UTF_8, ContentCoding.identity),
    )
    val uri     = descriptor.procedure.stripPrefix("/").split('/').foldLeft(baseUri)(_.addPath(_))
    val headers = ctx.headers.toHttp4s
      .put(entity.headers.map(Header.ToRaw.keyValuesToRaw).toSeq)
      .put(
        `User-Agent`(ProductId(userAgent)),
        ctx.timeout.map(`Connect-Timeout-Ms`.apply),
      )
    val httpRequest = Request[F](
      method = Method.POST,
      uri = uri,
      headers = headers,
      body = entity.body,
    )

    val response = httpClient.run(httpRequest).use { response =>
      val (headers, trailers) = response.headers.toConnectWithTrailers
      val responseEntity      = EntityToDecode[F](response.body, headers)

      def decode[A <: GeneratedMessage](
        companion: GeneratedMessageCompanion[A],
        codec: MessageCodec[F],
      ): F[A] = Async[F].defer(responseEntity.as[A](using companion, codec).compile.onlyOrError)

      if (response.status.isSuccess) {
        val mediaType = response.contentType.map(_.mediaType)
        if (mediaType.exists(_.satisfies(codecs.chosen.mediaType))) {
          decode(descriptor.responseCompanion, codecs.chosen)
            .map(message => ConnectResponse(message, headers, trailers))
            .adaptError { case error =>
              ConnectException(
                connectrpc.Code.Internal,
                Option(error.getMessage).getOrElse("Unable to decode the response"),
                headers = headers,
                trailers = trailers,
              ).withCause(error)
            }
        } else {
          val isConnectCodec = mediaType.exists { received =>
            received.satisfies(MediaTypes.`application/json`) ||
            received.satisfies(MediaTypes.`application/proto`)
          }
          val code =
            if (isConnectCodec) connectrpc.Code.Internal else codeByHttpStatus(response.status.code)
          ConnectException(
            code,
            s"Expected response content type ${codecs.chosen.mediaType.show}, got ${mediaType.map(_.show).getOrElse("<missing>")}",
            headers = headers,
            trailers = trailers,
          ).raiseError
        }
      } else {
        decode(connectrpc.Error, codecs.json)
          .attempt
          .flatMap {
            case Right(error) =>
              ConnectException(
                code = if (error.code.isUnspecified) codeByHttpStatus(response.status.code) else error.code,
                message = error.getMessage,
                details = error.details,
                headers = headers,
                trailers = trailers,
              ).raiseError
            case Left(decodeFailure) =>
              ConnectException(
                code = codeByHttpStatus(response.status.code),
                message = Option(decodeFailure.getMessage).getOrElse(response.status.reason),
                headers = headers,
                trailers = trailers,
              ).withCause(decodeFailure).raiseError
          }
      }
    }

    ctx.timeout.fold(response) { duration =>
      response.timeoutTo(
        duration,
        ConnectException(connectrpc.Code.DeadlineExceeded, "Request deadline exceeded").raiseError,
      )
    }
  }

  private def codeByHttpStatus(status: Int): connectrpc.Code = status match {
    case 400 => connectrpc.Code.Internal
    case 401 => connectrpc.Code.Unauthenticated
    case 403 => connectrpc.Code.PermissionDenied
    case 404 => connectrpc.Code.Unimplemented
    case 429 => connectrpc.Code.Unavailable
    case 502 => connectrpc.Code.Unavailable
    case 503 => connectrpc.Code.Unavailable
    case 504 => connectrpc.Code.Unavailable
    case _   => connectrpc.Code.Unknown
  }
}
