package me.ivovk.connect_rpc_scala.http4s.client

import cats.effect.{Async, Resource}
import cats.effect.syntax.all.*
import cats.implicits.*
import fs2.Stream
import org.http4s.client.Client
import org.http4s.headers.{`Accept-Encoding`, `User-Agent`}
import org.http4s.{ContentCoding, Headers, HttpVersion, Method, ProductId, Request, Response, Uri}
import me.ivovk.connect_rpc_scala.http.{ConnectHttpStatus, MediaTypes}
import me.ivovk.connect_rpc_scala.http.codec.*
import me.ivovk.connect_rpc_scala.http.headers.UnaryTrailers.*
import me.ivovk.connect_rpc_scala.http.headers.{`Connect-Accept-Encoding`, `Connect-Timeout-Ms`}
import me.ivovk.connect_rpc_scala.*
import scalapb.GeneratedMessage

import java.nio.charset.StandardCharsets

private[http4s] final class Transport[F[_]: Async](
  httpClient: Client[F],
  codecs: CodecRegistry[F],
  encoding: MessageEncoding,
  compression: ContentCoding,
  baseUri: Uri,
  userAgent: String,
) extends ConnectTransport[F] {

  private val unaryCodec     = codecs.unary(encoding)
  private val streamingCodec = codecs.streaming(encoding)

  private val unaryHeaders = Headers(
    `User-Agent`(ProductId(userAgent)),
    `Accept-Encoding`(Compressor.supportedCompressions),
  )
  private val streamingHeaders = Headers(
    `User-Agent`(ProductId(userAgent)),
    `Connect-Accept-Encoding`(Compressor.supportedCompressions),
  )

  override def unary[
    RequestMessage <: GeneratedMessage,
    ResponseMessage <: GeneratedMessage,
  ](
    descriptor: UnaryMethodDescriptor[RequestMessage, ResponseMessage],
    request: RequestMessage,
    ctx: ConnectRequestContext,
  ): F[ConnectResponse[ResponseMessage]] = {
    val uri     = baseUri / descriptor.serviceName / descriptor.methodName
    val headers = unaryHeaders ++ Headers(ctx.timeout.map(`Connect-Timeout-Ms`.apply)) ++ ctx.headers

    val encodeOptions = EncodeOptions(StandardCharsets.UTF_8, compression)

    val httpResponse = Resource.eval(unaryCodec.encode(request, encodeOptions))
      .map { entity =>
        Request[F](Method.POST, uri, HttpVersion.`HTTP/1.1`, headers ++ entity.headers, entity.body)
      }
      .flatMap(httpClient.run)

    val response = httpResponse.use { response =>
      val (headers, trailers) = response.headers.splitTrailers

      if (response.status.isSuccess) {
        val mediaType = response.contentType.map(_.mediaType)
        if (mediaType.exists(_.satisfies(unaryCodec.mediaType))) {
          val options = DecodeOptions.fromHeaders(headers)
          unaryCodec.decode(response.body, options)(using descriptor.responseCompanion)
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
            s"Expected response content type ${unaryCodec.mediaType.show}, got ${mediaType.map(_.show).getOrElse("<missing>")}",
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
    val headers = streamingHeaders ++ Headers(ctx.timeout.map(`Connect-Timeout-Ms`.apply)) ++ ctx.headers

    val httpRequest = Resource.eval(streamingCodec.encodeRequest(Stream.emit(request), compression))
      .map { entity =>
        Request[F](
          method = Method.POST,
          uri = baseUri / descriptor.serviceName / descriptor.methodName,
          headers = headers ++ entity.headers,
          body = entity.body,
        )
      }

    httpRequest.flatMap(httpClient.run).evalMap { response =>
      if !response.status.isSuccess then decodeHttpError(response)
      else if !response.contentType.exists(_.mediaType.satisfies(streamingCodec.mediaType)) then
        ConnectException(
          connectrpc.Code.Internal,
          s"Unexpected streaming response content type: ${response.contentType.map(_.mediaType.show).getOrElse("<missing>")}",
          headers = response.headers,
        ).raiseError
      else {
        val decoded =
          streamingCodec.decodeResponse(response.body, response.headers)(using descriptor.responseCompanion)
        val stream = ctx.timeout.fold(decoded) { duration =>
          decoded.timeoutTo(
            duration,
            ConnectException(connectrpc.Code.DeadlineExceeded, "Request deadline exceeded"),
          )
        }
        Async[F].pure(
          ConnectStreamingResponse(
            stream = stream,
            headers = response.headers,
          )
        )
      }
    }
  }

  private def decodeHttpError[A](response: Response[F]): F[A] = {
    val (headers, trailers) = response.headers.splitTrailers

    codecs.json.decode[connectrpc.Error](response.body, DecodeOptions.fromHeaders(headers))
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
