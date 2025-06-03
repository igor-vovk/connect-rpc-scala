package org.ivovk.connect_rpc_scala.http4s.connect

import cats.MonadThrow
import cats.data.OptionT
import cats.implicits.*
import org.http4s.Status.UnsupportedMediaType
import org.http4s.{Headers, HttpRoutes, MediaType, Method, Response}
import org.ivovk.connect_rpc_scala.grpc.MethodRegistry
import org.ivovk.connect_rpc_scala.http.codec.{EntityToDecode, MessageCodec, MessageCodecRegistry}
import org.ivovk.connect_rpc_scala.http.{HeadersToMetadata, MediaTypes, Paths}
import org.ivovk.connect_rpc_scala.http4s.Conversions.http4sPathToConnectRpcPath

class ConnectRoutesProvider[F[_]: MonadThrow](
  pathPrefix: Paths.Path,
  methodRegistry: MethodRegistry,
  codecRegistry: MessageCodecRegistry[F],
  headerMapping: HeadersToMetadata[Headers],
  handler: ConnectHandler[F],
) {
  private val OptionTNone: OptionT[F, Response[F]] = OptionT.none[F, Response[F]]

  def routes: HttpRoutes[F] = HttpRoutes[F] { req =>
    val aGetMethod = req.method == Method.GET

    if !(aGetMethod || req.method == Method.POST) then OptionTNone
    else {
      val pathParts = Paths.dropPrefix(http4sPathToConnectRpcPath(req.uri.path), pathPrefix)

      OptionT
        .fromOption[F](pathParts match {
          case Some(service :: method :: Nil) =>
            // Temporary support GET-requests for all methods,
            // until https://github.com/scalapb/ScalaPB/pull/1774 is released
            methodRegistry.get(service, method) // .filter(_.descriptor.isSafe || aGetMethod)
          case _ =>
            None
        })
        .semiflatMap { methodEntry =>
          val query     = req.uri.query
          val mediaType =
            if aGetMethod then
              query.multiParams.get("encoding")
                .flatMap(_.headOption)
                .flatMap(MediaTypes.parseShort(_).toOption)
            else req.contentType.map(_.mediaType)

          val message: String | fs2.Stream[F, Byte] =
            if aGetMethod then req.multiParams.get("message").flatMap(_.headOption).getOrElse("")
            else req.body

          val entity = EntityToDecode[F](message, headerMapping.toMetadata(req.headers))

          withCodec(codecRegistry, mediaType) { codec =>
            handler.handle(entity, methodEntry)(using codec)
          }
        }
    }
  }

  private def withCodec(
    registry: MessageCodecRegistry[F],
    mediaType: Option[MediaType],
  )(r: MessageCodec[F] => F[Response[F]]): F[Response[F]] =
    mediaType.flatMap(registry.byMediaType) match {
      case Some(codec) => r(codec)
      case None        =>
        val message = s"Unsupported media-type ${mediaType.show}. " +
          s"Supported media types: ${MediaTypes.allSupported.map(_.show).mkString(", ")}"

        Response(UnsupportedMediaType).withEntity(message).pure[F]
    }

}
