package me.ivovk.connect_rpc_scala.http4s.server

import cats.effect.Async
import cats.effect.syntax.all.*
import cats.implicits.*
import fs2.Stream
import me.ivovk.connect_rpc_scala.http.ConnectHttpStatus
import me.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, MessageCodec}
import me.ivovk.connect_rpc_scala.http4s.headers.UnaryTrailers.*
import me.ivovk.connect_rpc_scala.http4s.headers.`Connect-Timeout-Ms`
import me.ivovk.connect_rpc_scala.service.*
import org.http4s.{ContentCoding, Headers, Response, Status}
import scalapb.GeneratedMessage

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

private[http4s] final class UnaryHandler[F[_]: Async](
  treatTrailersAsHeaders: Boolean,
  jsonCodec: MessageCodec[F],
) {

  def invokeAndEncode(
    method: UnaryMethod[F],
    request: method.RequestMessage,
    headers: Headers,
    codec: MessageCodec[F],
  ): F[Response[F]] =
    invoke(method, request, ConnectRequestContext(headers, timeout(headers)))
      .flatMap(result => encodeResult(result, codec))

  def decodeError(error: Throwable): ConnectException =
    ConnectException(
      connectrpc.Code.InvalidArgument,
      Option(error.getMessage).getOrElse("Unable to decode the request"),
    ).withCause(error)

  def errorResponse(error: Throwable): F[Response[F]] = {
    val exception = error match {
      case connect: ConnectException => connect
      case other                     =>
        ConnectException(
          connectrpc.Code.Internal,
          Option(other.getMessage).getOrElse("Internal error"),
        ).withCause(other)
    }

    val status  = Status.fromInt(ConnectHttpStatus.fromCode(exception.code)).fold(throw _, identity)
    val headers = Headers(exception.headers.headers ++ exception.trailers.headers)
    val body    = connectrpc.Error(
      code = exception.code,
      message = Option(exception.getMessage).filter(_.nonEmpty),
      details = exception.details,
    )

    encodeResponse(status, headers, body, jsonCodec)
  }

  private def invoke(
    method: UnaryMethod[F],
    request: method.RequestMessage,
    ctx: ConnectRequestContext,
  ): F[ConnectResponse[method.ResponseMessage]] = {
    val response = method.invoke(request, ctx)
    ctx.timeout.fold(response) { duration =>
      response.timeoutTo(
        duration,
        ConnectException(connectrpc.Code.DeadlineExceeded, "Request deadline exceeded").raiseError,
      )
    }
  }

  private def encodeResult(
    result: ConnectResponse[? <: GeneratedMessage],
    codec: MessageCodec[F],
  ): F[Response[F]] = {
    val trailerHeaders =
      if (treatTrailersAsHeaders) result.trailers else result.trailers.withTrailerPrefix
    val responseHeaders = result.headers ++ trailerHeaders

    encodeResponse(Status.Ok, responseHeaders, result.message, codec)
  }

  private def encodeResponse(
    status: Status,
    headers: Headers,
    message: GeneratedMessage,
    codec: MessageCodec[F],
  ): F[Response[F]] = {
    val entity = codec.encode(
      Stream.emit(message),
      EncodeOptions(StandardCharsets.UTF_8, ContentCoding.identity),
    )
    Async[F].pure(
      Response[F](
        status = status,
        headers = headers ++ entity.headers,
        body = entity.body,
      )
    )
  }

  private def timeout(headers: Headers): Option[FiniteDuration] =
    headers.get[`Connect-Timeout-Ms`].map(_.timeout)
}
