package org.ivovk.connect_rpc_scala.grpc_web

import cats.MonadThrow
import cats.data.OptionT
import cats.implicits.*
import org.http4s.Status.UnsupportedMediaType
import org.http4s.dsl.request.*
import org.http4s.{HttpRoutes, MediaType, Method, Response, Uri}
import org.ivovk.connect_rpc_scala.grpc.MethodRegistry
import org.ivovk.connect_rpc_scala.http.codec.{MessageCodec, MessageCodecRegistry}
import org.ivovk.connect_rpc_scala.http.{MediaTypes, RequestEntity}

class GrpcWebRoutesProvider[F[_] : MonadThrow](
  pathPrefix: Uri.Path,
  methodRegistry: MethodRegistry,
  codecRegistry: MessageCodecRegistry[F],
  handler: GrpcWebHandler[F],
) {

  private val SupportedMediaTypes: Seq[MediaType] = List(
    MediaTypes.`application/grpc-web+json`
  )

  def routes: HttpRoutes[F] = HttpRoutes[F] {
    case req@Method.POST -> `pathPrefix` / service / method =>
      req.contentType.map(_.mediaType) match {
        case Some(MediaTypes.`application/grpc-web+json`) =>
          OptionT.fromOption[F](methodRegistry.get(service, method))
            .semiflatMap { methodEntry =>
              withCodec(codecRegistry, MediaTypes.`application/json`.some) { codec =>
                val entity = RequestEntity.fromBody(req)

                handler.handle(entity, methodEntry)(using codec)
              }
            }
        case _ =>
          OptionT.none
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
