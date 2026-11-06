package org.ivovk.connect_rpc_scala.http4s.server

import cats.effect.Async
import cats.effect.syntax.all.*
import cats.implicits.*
import fs2.Stream
import org.http4s.{ContentCoding, Header, Headers, Request, Response, Status}
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, MessageCodec}
import org.ivovk.connect_rpc_scala.service.*
import org.typelevel.ci.CIString
import scalapb.GeneratedMessage

import java.nio.charset.Charset
import scala.concurrent.duration.*

private[http4s] final class UnaryHandler[F[_]: Async](
  incomingHeadersFilter: String => Boolean,
  outgoingHeadersFilter: String => Boolean,
  treatTrailersAsHeaders: Boolean,
) {

  def connectHeaders(request: Request[F]): ConnectHeaders =
    ConnectHeaders.create(
      request.headers.headers.iterator
        .map(header => header.name.toString -> header.value)
        .filter((name, _) => incomingHeadersFilter(name))
        .toSeq
    )

  def invokeAndEncode(
    method: UnaryMethod[F],
    request: method.RequestMessage,
    headers: ConnectHeaders,
    codec: MessageCodec[F],
  ): F[Response[F]] =
    invoke(method, request, ConnectRequestContext(headers, timeout(headers)))
      .flatMap(result => encodeResult(result, codec))

  def decodeError(error: Throwable): ConnectException =
    ConnectException(
      connectrpc.Code.InvalidArgument,
      Option(error.getMessage).getOrElse("Unable to decode the request"),
    ).withCause(error)

  def errorResponse(error: Throwable, codec: MessageCodec[F]): F[Response[F]] = {
    val exception = error match {
      case connect: ConnectException => connect
      case other                     =>
        ConnectException(
          connectrpc.Code.Internal,
          Option(other.getMessage).getOrElse("Internal error"),
        ).withCause(other)
    }

    val status  = Status.fromInt(httpStatus(exception.code)).fold(throw _, identity)
    val headers = toHttp4sHeaders(exception.headers ++ exception.trailers)
    val body    = connectrpc.Error(
      code = exception.code,
      message = Option(exception.getMessage).filter(_.nonEmpty),
      details = exception.details,
    )
    encodeResponse(status, headers, body, codec)
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
    val responseHeaders = toHttp4sHeaders(result.headers) ++ toHttp4sTrailers(result.trailers)
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
      EncodeOptions(Charset.defaultCharset(), ContentCoding.identity),
    )
    Async[F].pure(
      Response[F](
        status = status,
        headers = headers.put(entity.headers.map(Header.ToRaw.keyValuesToRaw).toSeq*),
        body = entity.body,
      )
    )
  }

  private def timeout(headers: ConnectHeaders): Option[FiniteDuration] =
    headers.get("connect-timeout-ms")
      .flatMap(_.headOption)
      .flatMap(_.toLongOption)
      .map(_.millis)

  private def toHttp4sHeaders(headers: ConnectHeaders): Headers =
    toHttp4sHeaders(headers, trailing = false)

  private def toHttp4sTrailers(headers: ConnectHeaders): Headers =
    toHttp4sHeaders(headers, trailing = !treatTrailersAsHeaders)

  private def toHttp4sHeaders(headers: ConnectHeaders, trailing: Boolean): Headers = {
    val raw = headers.entries.toSeq.flatMap { (name, values) =>
      if outgoingHeadersFilter(name) then {
        val outputName = if trailing then s"trailer-$name" else name
        values.map(value => Header.Raw(CIString(outputName), value))
      } else Seq.empty
    }
    new Headers(raw.toList)
  }

  private def httpStatus(code: connectrpc.Code): Int = code match {
    case connectrpc.Code.Canceled           => 499
    case connectrpc.Code.InvalidArgument    => 400
    case connectrpc.Code.DeadlineExceeded   => 504
    case connectrpc.Code.NotFound           => 404
    case connectrpc.Code.AlreadyExists      => 409
    case connectrpc.Code.PermissionDenied   => 403
    case connectrpc.Code.ResourceExhausted  => 429
    case connectrpc.Code.FailedPrecondition => 400
    case connectrpc.Code.Aborted            => 409
    case connectrpc.Code.OutOfRange         => 400
    case connectrpc.Code.Unimplemented      => 501
    case connectrpc.Code.Unavailable        => 503
    case connectrpc.Code.Unauthenticated    => 401
    case _                                  => 500
  }
}
