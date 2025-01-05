package org.ivovk.connect_rpc_scala.connect

import cats.MonadThrow
import cats.data.OptionT
import cats.implicits.*
import org.http4s.Status.UnsupportedMediaType
import org.http4s.dsl.request.*
import org.http4s.{HttpRoutes, MediaType, Method, Response, Uri}
import org.ivovk.connect_rpc_scala.grpc.MethodRegistry
import org.ivovk.connect_rpc_scala.http.MediaTypes.{`application/json`, `application/proto`}
import org.ivovk.connect_rpc_scala.http.QueryParams.*
import org.ivovk.connect_rpc_scala.http.codec.{MessageCodec, MessageCodecRegistry}
import org.ivovk.connect_rpc_scala.http.{MediaTypes, RequestEntity}

class ConnectRoutesProvider[F[_] : MonadThrow](
  pathPrefix: Uri.Path,
  methodRegistry: MethodRegistry,
  codecRegistry: MessageCodecRegistry[F],
  handler: ConnectHandler[F],
) {

  private val SupportedMediaTypes: Seq[MediaType] = List(`application/json`, `application/proto`)

  def routes: HttpRoutes[F] = HttpRoutes[F] {
    case req@Method.GET -> `pathPrefix` / service / method :? EncodingQP(mediaType) +& MessageQP(message) =>
      OptionT.fromOption[F](methodRegistry.get(service, method))
        // Temporary support GET-requests for all methods,
        // until https://github.com/scalapb/ScalaPB/pull/1774 is merged
        .filter(_.descriptor.isSafe || true)
        .semiflatMap { methodEntry =>
          withCodec(codecRegistry, mediaType.some) { codec =>
            val entity = RequestEntity[F](message, req.headers)

            handler.handle(entity, methodEntry)(using codec)
          }
        }
    case req@Method.POST -> `pathPrefix` / service / method =>
      OptionT.fromOption[F](methodRegistry.get(service, method))
        .semiflatMap { methodEntry =>
          withCodec(codecRegistry, req.contentType.map(_.mediaType)) { codec =>
            val entity = RequestEntity.fromBody(req)

            handler.handle(entity, methodEntry)(using codec)
          }
        }
    case _ =>
      OptionT.none
  }

  private def withCodec(
    registry: MessageCodecRegistry[F],
    mediaType: Option[MediaType]
  )(r: MessageCodec[F] => F[Response[F]]): F[Response[F]] = {
    mediaType.flatMap(registry.byMediaType) match {
      case Some(codec) => r(codec)
      case None =>
        val message = s"Unsupported media-type ${mediaType.show}. " +
          s"Supported media types: ${SupportedMediaTypes.map(_.show).mkString(", ")}"

        Response(UnsupportedMediaType).withEntity(message).pure[F]
    }
  }

}
