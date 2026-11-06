package org.ivovk.connect_rpc_scala.http4s

import cats.Endo
import cats.effect.Async
import cats.effect.syntax.all.*
import cats.implicits.*
import fs2.Stream
import org.http4s.client.Client
import org.http4s.{ContentCoding, Header, Headers, Method, Request, Uri}
import org.ivovk.connect_rpc_scala.http.codec.*
import org.ivovk.connect_rpc_scala.service.*
import org.typelevel.ci.CIString
import scalapb.GeneratedMessage

import java.nio.charset.Charset

object ConnectHttp4sClientBuilder {

  def apply[F[_]: Async](client: Client[F]): ConnectHttp4sClientBuilder[F] = {
    val messageCodec = JsonSerdesBuilder[F]().build.codec
    new ConnectHttp4sClientBuilder(
      client = client,
      messageCodec = messageCodec,
    )
  }
}

final class ConnectHttp4sClientBuilder[F[_]: Async] private (
  client: Client[F],
  messageCodec: MessageCodec[F],
) {

  private def copy(newMessageCodec: MessageCodec[F]): ConnectHttp4sClientBuilder[F] =
    new ConnectHttp4sClientBuilder(client, newMessageCodec)

  def withJsonCodecConfigurator(
    configure: Endo[JsonSerdesBuilder[F]]
  ): ConnectHttp4sClientBuilder[F] =
    copy(configure(JsonSerdesBuilder[F]()).build.codec)

  def enableBinaryFormat(): ConnectHttp4sClientBuilder[F] =
    copy(new ProtoMessageCodec[F])

  def build(baseUri: Uri): UnaryTransport[F] =
    ConnectHttp4sUnaryTransport(client, messageCodec, baseUri)
}

private final class ConnectHttp4sUnaryTransport[F[_]: Async](
  httpClient: Client[F],
  messageCodec: MessageCodec[F],
  baseUri: Uri,
) extends UnaryTransport[F] {

  override def unary[
    RequestMessage <: GeneratedMessage,
    ResponseMessage <: GeneratedMessage,
  ](
    descriptor: UnaryMethodDescriptor[RequestMessage, ResponseMessage],
    request: RequestMessage,
    ctx: ConnectRequestContext,
  ): F[ConnectResponse[ResponseMessage]] = {
    val entity = messageCodec.encode(
      Stream.emit(request),
      EncodeOptions(Charset.defaultCharset(), ContentCoding.identity),
    )
    val uri     = descriptor.procedure.stripPrefix("/").split('/').foldLeft(baseUri)(_.addPath(_))
    val headers = toHttp4sHeaders(ctx.headers)
      .put(entity.headers.map(Header.ToRaw.keyValuesToRaw).toSeq*)
      .put(Header.Raw(CIString("x-user-agent"), "connect-rpc-scala-http4s"))
      .put(
        ctx.timeout.map(timeout => Header.Raw(CIString("connect-timeout-ms"), timeout.toMillis.toString))
      )
    val httpRequest = Request[F](
      method = Method.POST,
      uri = uri,
      headers = headers,
      body = entity.body,
    )

    val response = httpClient.run(httpRequest).use { response =>
      val (connectHeaders, connectTrailers) = fromHttp4sHeaders(response.headers)
      val responseEntity                    = EntityToDecode[F](response.body, connectHeaders)

      if response.status.isSuccess then
        responseEntity.as[ResponseMessage](using descriptor.responseCompanion, messageCodec)
          .compile.onlyOrError
          .map(message => ConnectResponse(message, connectHeaders, connectTrailers))
      else
        responseEntity.as[connectrpc.Error](using connectrpc.Error, messageCodec)
          .compile.onlyOrError
          .attempt
          .flatMap {
            case Right(error) =>
              ConnectException(
                code =
                  if error.code.isUnspecified then codeByHttpStatus(response.status.code) else error.code,
                message = error.getMessage,
                details = error.details,
                headers = connectHeaders,
                trailers = connectTrailers,
              ).raiseError
            case Left(decodeFailure) =>
              ConnectException(
                code = codeByHttpStatus(response.status.code),
                message = Option(decodeFailure.getMessage).getOrElse(response.status.reason),
                headers = connectHeaders,
                trailers = connectTrailers,
              ).withCause(decodeFailure).raiseError
          }
    }

    ctx.timeout.fold(response) { duration =>
      response.timeoutTo(
        duration,
        ConnectException(connectrpc.Code.DeadlineExceeded, "Request deadline exceeded").raiseError,
      )
    }
  }

  private def toHttp4sHeaders(headers: ConnectHeaders): Headers = {
    val raw = headers.entries.toSeq.flatMap { case (name, values) =>
      values.map(value => Header.Raw(CIString(name), value))
    }
    new Headers(raw.toList)
  }

  private def fromHttp4sHeaders(headers: Headers): (ConnectHeaders, ConnectHeaders) = {
    val pairs              = headers.headers.map(header => header.name.toString -> header.value)
    val (trailers, normal) = pairs.partition(_._1.toLowerCase(java.util.Locale.ROOT).startsWith("trailer-"))
    val connectTrailers    = trailers.map { case (name, value) => name.drop("trailer-".length) -> value }
    ConnectHeaders.create(normal) -> ConnectHeaders.create(connectTrailers)
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
