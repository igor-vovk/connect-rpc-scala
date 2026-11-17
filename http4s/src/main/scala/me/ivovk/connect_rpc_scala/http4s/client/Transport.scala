package me.ivovk.connect_rpc_scala.http4s.client

import cats.effect.{Async, Resource}
import cats.effect.syntax.all.*
import cats.implicits.*
import fs2.Stream
import org.http4s.client.Client
import org.http4s.headers.`User-Agent`
import org.http4s.{ContentCoding, Headers, Method, ProductId, Request, Response, Uri}
import me.ivovk.connect_rpc_scala.http.{ConnectHttpStatus, MediaTypes}
import me.ivovk.connect_rpc_scala.http.codec.*
import me.ivovk.connect_rpc_scala.http4s.headers.UnaryTrailers.*
import me.ivovk.connect_rpc_scala.http4s.headers.`Connect-Timeout-Ms`
import me.ivovk.connect_rpc_scala.service.*
import scalapb.GeneratedMessage

import java.nio.charset.StandardCharsets

private[http4s] final class Transport[F[_]: Async](
  httpClient: Client[F],
  codecs: ClientCodecs[F],
  baseUri: Uri,
  userAgent: String,
) extends ConnectTransport[F] {

  override def unary[
    RequestMessage <: GeneratedMessage,
    ResponseMessage <: GeneratedMessage,
  ](
    descriptor: UnaryMethodDescriptor[RequestMessage, ResponseMessage],
    request: RequestMessage,
    ctx: ConnectRequestContext,
  ): F[ConnectResponse[ResponseMessage]] = {
    val uri         = baseUri / descriptor.serviceName / descriptor.methodName
    val httpRequest = Request[F](
      method = Method.POST,
      uri = uri,
      headers = Headers(
        `User-Agent`(ProductId(userAgent)),
        ctx.timeout.map(`Connect-Timeout-Ms`.apply),
      ) ++ ctx.headers,
    )

    val encodeOptions = EncodeOptions(StandardCharsets.UTF_8, ContentCoding.identity)

    val httpResponse = Resource.eval(codecs.chosen.encode(request, encodeOptions))
      .map { entity =>
        httpRequest.withBodyStream(entity.body).transformHeaders(_ ++ entity.headers)
      }
      .flatMap(httpClient.run)

    val response = httpResponse.use { response =>
      val (headers, trailers) = response.headers.splitTrailers
      val responseEntity      = EntityToDecode[F](response.body, headers)

      if (response.status.isSuccess) {
        val mediaType = response.contentType.map(_.mediaType)
        if (mediaType.exists(_.satisfies(codecs.chosen.mediaType))) {
          responseEntity.decodeOne(descriptor.responseCompanion, codecs.chosen)
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
            received.satisfies(MediaTypes.`application/json`)
            || received.satisfies(MediaTypes.`application/proto`)
          }

          val code =
            if (isConnectCodec) connectrpc.Code.Internal else ConnectHttpStatus.toCode(response.status.code)

          ConnectException(
            code,
            s"Expected response content type ${codecs.chosen.mediaType.show}, got ${mediaType.map(_.show).getOrElse("<missing>")}",
            headers = headers,
            trailers = trailers,
          ).raiseError
        }
      } else {
        decodeHttpError(response)
      }
    }

    ctx.timeout.fold(response) { duration =>
      response.timeoutTo(
        duration,
        ConnectException(connectrpc.Code.DeadlineExceeded, "Request deadline exceeded").raiseError,
      )
    }
  }

  override def serverStreaming[
    RequestMessage <: GeneratedMessage,
    ResponseMessage <: GeneratedMessage,
  ](
    descriptor: ServerStreamingMethodDescriptor[RequestMessage, ResponseMessage],
    request: RequestMessage,
    ctx: ConnectRequestContext,
  ): Resource[F, ConnectStreamingResponse[F, ResponseMessage]] = {
    val streamingCodec = new StreamingMessageCodec(codecs.chosen, codecs.jsonSerdes)
    val entity         = streamingCodec.encodeRequest(Stream.emit(request))
    val httpRequest    = Request[F](
      method = Method.POST,
      uri = baseUri / descriptor.serviceName / descriptor.methodName,
      headers = Headers(
        `User-Agent`(ProductId(userAgent)),
        ctx.timeout.map(`Connect-Timeout-Ms`.apply),
      ) ++ ctx.headers ++ entity.headers,
      body = entity.body,
    )

    httpClient.run(httpRequest).evalMap { response =>
      val responseHeaders = response.headers

      if !response.status.isSuccess then decodeHttpError(response)
      else if !response.contentType.exists(_.mediaType.satisfies(streamingCodec.mediaType)) then
        ConnectException(
          connectrpc.Code.Internal,
          s"Unexpected streaming response content type: ${response.contentType.map(_.mediaType.show).getOrElse("<missing>")}",
          headers = responseHeaders,
        ).raiseError
      else {
        given scalapb.GeneratedMessageCompanion[ResponseMessage] = descriptor.responseCompanion
        val decoded = streamingCodec.decodeResponse[ResponseMessage](response.body, responseHeaders)
        val stream  = ctx.timeout.fold(decoded) { duration =>
          decoded.timeoutTo(
            duration,
            ConnectException(connectrpc.Code.DeadlineExceeded, "Request deadline exceeded"),
          )
        }
        Async[F].pure(
          ConnectStreamingResponse(
            stream = stream,
            headers = responseHeaders,
          )
        )
      }
    }
  }

  private def decodeHttpError[A](response: Response[F]): F[A] = {
    val (headers, trailers) = response.headers.splitTrailers
    val responseEntity      = EntityToDecode[F](response.body, headers)

    responseEntity.decodeOne(connectrpc.Error, codecs.json)
      .attempt
      .flatMap {
        case Right(error) =>
          ConnectException(
            code =
              if error.code.isUnspecified then ConnectHttpStatus.toCode(response.status.code) else error.code,
            message = error.getMessage,
            details = error.details,
            headers = headers,
            trailers = trailers,
          ).raiseError
        case Left(decodeFailure) =>
          ConnectException(
            code = ConnectHttpStatus.toCode(response.status.code),
            message = Option(decodeFailure.getMessage).getOrElse(response.status.reason),
            headers = headers,
            trailers = trailers,
          ).withCause(decodeFailure).raiseError
      }
  }

}
