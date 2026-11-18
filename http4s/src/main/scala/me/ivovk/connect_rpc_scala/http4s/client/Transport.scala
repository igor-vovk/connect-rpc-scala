package me.ivovk.connect_rpc_scala.http4s.client

import cats.effect.{Async, Resource}
import cats.effect.syntax.all.*
import cats.implicits.*
import org.http4s.client.Client
import org.http4s.headers.`User-Agent`
import org.http4s.{ContentCoding, Method, ProductId, Request, Uri}
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
) extends UnaryTransport[F] {

  override def unary[
    RequestMessage <: GeneratedMessage,
    ResponseMessage <: GeneratedMessage,
  ](
    descriptor: UnaryMethodDescriptor[RequestMessage, ResponseMessage],
    request: RequestMessage,
    ctx: ConnectRequestContext,
  ): F[ConnectResponse[ResponseMessage]] = {
    val uri     = descriptor.procedure.stripPrefix("/").split('/').foldLeft(baseUri)(_.addPath(_))
    val encoded = Resource.eval(
      codecs.chosen.encodeUnary(request, EncodeOptions(StandardCharsets.UTF_8, ContentCoding.identity))
    )
    val httpResponse = encoded.flatMap { entity =>
      val headers = entity.headersWithMetadata(ctx.headers)
        .put(
          `User-Agent`(ProductId(userAgent)),
          ctx.timeout.map(`Connect-Timeout-Ms`.apply),
        )
      httpClient.run(Request[F](method = Method.POST, uri = uri, headers = headers, body = entity.body))
    }

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
        responseEntity.decodeOne(connectrpc.Error, codecs.json)
          .attempt
          .flatMap {
            case Right(error) =>
              ConnectException(
                code =
                  if (error.code.isUnspecified) ConnectHttpStatus.toCode(response.status.code)
                  else error.code,
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

    ctx.timeout.fold(response) { duration =>
      response.timeoutTo(
        duration,
        ConnectException(connectrpc.Code.DeadlineExceeded, "Request deadline exceeded").raiseError,
      )
    }
  }
}
