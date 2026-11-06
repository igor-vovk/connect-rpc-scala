package org.ivovk.connect_rpc_scala.http4s

import cats.data.OptionT
import cats.effect.Async
import cats.implicits.*
import fs2.Stream
import org.http4s.Status.UnsupportedMediaType
import org.http4s.{HttpRoutes, Method, Request, Response, Uri}
import org.ivovk.connect_rpc_scala.http.MediaTypes
import org.ivovk.connect_rpc_scala.http.codec.*
import org.ivovk.connect_rpc_scala.service.{ServiceDefinition, UnaryMethod}

private[http4s] final class ConnectHttp4sConnectRoutes[F[_]: Async](
  services: Seq[ServiceDefinition[F]],
  jsonSerdes: JsonSerdes[F],
  pathPrefix: Uri.Path,
  unaryHandler: ConnectHttp4sUnaryHandler[F],
) {

  private val protoCodec = new ProtoMessageCodec[F]

  private val methodsByPath: Map[(String, String), UnaryMethod[F]] = {
    val methods = services.flatMap(service => service.methods.map(method => (service.name, method)))
    val result  = methods.map { case (serviceName, method) =>
      (serviceName -> method.descriptor.methodName) -> method
    }.toMap

    require(result.size == methods.size, "Duplicate Connect service or method definition")
    result
  }

  val routes: HttpRoutes[F] =
    HttpRoutes[F] { request =>
      findMethod(request).fold(OptionT.none[F, Response[F]]) { method =>
        OptionT.liftF(handle(request, method))
      }
    }

  private def findMethod(request: Request[F]): Option[UnaryMethod[F]] =
    if request.method != Method.GET && request.method != Method.POST then None
    else {
      val path   = request.uri.path.segments.map(_.encoded).toList
      val prefix = pathPrefix.segments.map(_.encoded).toList
      if path.take(prefix.size) != prefix then None
      else
        path.drop(prefix.size) match {
          case serviceName :: methodName :: Nil => methodsByPath.get(serviceName -> methodName)
          case _                                => None
        }
    }

  private def handle(request: Request[F], method: UnaryMethod[F]): F[Response[F]] = {
    val isGet = request.method == Method.GET
    codecFor(request, isGet) match {
      case None        => unsupportedMediaType(request, isGet)
      case Some(codec) =>
        val message: String | Stream[F, Byte] =
          if isGet then request.uri.query.multiParams.get("message").flatMap(_.headOption).getOrElse("")
          else request.body

        decodeAndInvoke(
          method,
          EntityToDecode(message, unaryHandler.connectHeaders(request)),
          codec,
        )
    }
  }

  private def decodeAndInvoke(
    method: UnaryMethod[F],
    entity: EntityToDecode[F],
    codec: MessageCodec[F],
  ): F[Response[F]] = {
    given scalapb.GeneratedMessageCompanion[method.RequestMessage] = method.descriptor.requestCompanion

    entity.as[method.RequestMessage](using summon, codec)
      .compile.onlyOrError
      .adaptError { case error => unaryHandler.decodeError(error) }
      .flatMap(message => unaryHandler.invokeAndEncode(method, message, entity.headers, codec))
      .handleErrorWith(error => unaryHandler.errorResponse(error, codec))
  }

  private def codecFor(request: Request[F], isGet: Boolean): Option[MessageCodec[F]] = {
    val mediaType =
      if isGet then
        request.uri.query.multiParams.get("encoding").flatMap(_.headOption).flatMap { value =>
          MediaTypes.parseShort(value).toOption
        }
      else request.contentType.map(_.mediaType)

    mediaType.flatMap {
      case MediaTypes.`application/json`  => Some(jsonSerdes.codec)
      case MediaTypes.`application/proto` => Some(protoCodec)
      case _                              => None
    }
  }

  private def unsupportedMediaType(request: Request[F], isGet: Boolean): F[Response[F]] = {
    val provided =
      if isGet then request.uri.query.multiParams.get("encoding").flatMap(_.headOption)
      else request.contentType.map(_.mediaType.show)
    val message = s"Unsupported media type or encoding ${provided.getOrElse("<missing>")}. " +
      "Supported values are application/json, application/proto, json, and proto."
    Async[F].pure(Response[F](UnsupportedMediaType).withEntity(message))
  }
}
