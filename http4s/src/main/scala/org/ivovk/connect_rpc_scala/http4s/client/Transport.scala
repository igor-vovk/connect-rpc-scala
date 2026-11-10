package org.ivovk.connect_rpc_scala.http4s.client

import cats.effect.Async
import cats.effect.syntax.all.*
import cats.implicits.*
import fs2.Stream
import org.http4s.client.Client
import org.http4s.headers.`User-Agent`
import org.http4s.{ContentCoding, Header, Method, ProductId, Request, Uri}
import org.ivovk.connect_rpc_scala.http.codec.*
import org.ivovk.connect_rpc_scala.http4s.headers.HeaderConverters.*
import org.ivovk.connect_rpc_scala.http4s.headers.`Connect-Timeout-Ms`
import org.ivovk.connect_rpc_scala.service.*
import scalapb.GeneratedMessage

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

      if response.status.isSuccess then
        responseEntity.as[ResponseMessage](using descriptor.responseCompanion, codecs.chosen)
          .compile.onlyOrError
          .map(message => ConnectResponse(message, headers, trailers))
      else
        responseEntity.as[connectrpc.Error](using connectrpc.Error, codecs.json)
          .compile.onlyOrError
          .attempt
          .flatMap {
            case Right(error) =>
              ConnectException(
                code =
                  if error.code.isUnspecified then codeByHttpStatus(response.status.code) else error.code,
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
