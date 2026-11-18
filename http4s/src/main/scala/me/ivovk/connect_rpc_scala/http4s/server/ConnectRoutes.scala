package me.ivovk.connect_rpc_scala.http4s.server

import cats.data.OptionT
import cats.effect.Async
import cats.implicits.*
import fs2.Stream
import org.http4s.Status.{MethodNotAllowed, UnsupportedMediaType}
import org.http4s.headers.{Allow, Connection}
import org.http4s.{ContentCoding, HttpRoutes, HttpVersion, Method, Request, Response, Uri}
import me.ivovk.connect_rpc_scala.http.MediaTypes
import me.ivovk.connect_rpc_scala.http.codec.*
import me.ivovk.connect_rpc_scala.http4s.headers.HeaderConverters.*
import me.ivovk.connect_rpc_scala.service.{ConnectException, ServiceDefinition, UnaryMethod}

object ConnectRoutes {
  // A rejected request may have unread body bytes. Tell HTTP/1 clients that the connection cannot be reused.
  extension [F[_]](response: Response[F]) {
    private def closeIfHttp1(version: HttpVersion): Response[F] =
      if (version.major == 1) response.putHeaders(Connection.close) else response
  }
}

private[http4s] final class ConnectRoutes[F[_]: Async](
  services: Seq[ServiceDefinition[F]],
  jsonSerdes: JsonSerdes[F],
  pathPrefix: Uri.Path,
  unaryHandler: UnaryHandler[F],
) {
  import ConnectRoutes.*

  private val protoCodec = new ProtoMessageCodec[F]

  private val methodsByPath: Map[(String, String), UnaryMethod[F]] = {
    val methods = services.flatMap(service => service.methods.map(method => (service.name, method)))
    val result  = methods.map { (serviceName, method) =>
      (serviceName -> method.descriptor.methodName) -> method
    }.toMap

    require(result.size == methods.size, "Duplicate Connect service or method definition")
    result
  }

  val routes: HttpRoutes[F] =
    HttpRoutes[F] { request =>
      findMethod(request.uri.path).fold(OptionT.none[F, Response[F]]) { method =>
        val response = request.method match {
          case Method.GET | Method.POST => handle(request, method)
          case _                        =>
            val rejected = Response[F](MethodNotAllowed).putHeaders(Allow(Method.GET, Method.POST))
            Async[F].pure(rejected.closeIfHttp1(request.httpVersion))
        }
        OptionT.liftF(response)
      }
    }

  private def findMethod(requestPath: Uri.Path): Option[UnaryMethod[F]] = {
    val path   = requestPath.segments.map(_.encoded).toList
    val prefix = pathPrefix.segments.map(_.encoded).toList
    if (path.take(prefix.size) != prefix) None
    else {
      path.drop(prefix.size) match {
        case serviceName :: methodName :: Nil => methodsByPath.get(serviceName -> methodName)
        case _                                => None
      }
    }
  }

  private def handle(request: Request[F], method: UnaryMethod[F]): F[Response[F]] = {
    val isGet = request.method.isSafe

    codecFor(request, isGet) match {
      case None        => unsupportedMediaType(request, isGet)
      case Some(codec) =>
        val message: String | Stream[F, Byte] =
          if (isGet) {
            request.uri.query.pairs
              .collectFirst { case ("message", value) => value }
              .flatten
              .getOrElse("")
          } else request.body

        decodeAndInvoke(
          method,
          EntityToDecode(message, request.headers.toConnect),
          codec,
          request.httpVersion,
        )
    }
  }

  private def decodeAndInvoke(
    method: UnaryMethod[F],
    entity: EntityToDecode[F],
    codec: MessageCodec[F],
    httpVersion: HttpVersion,
  ): F[Response[F]] =
    Async[F].defer {
      val encoding = entity.message match {
        case _: String => ContentCoding.identity
        case _         => entity.encoding
      }
      encoding match {
        case ContentCoding.identity | ContentCoding.gzip =>
          entity.decodeOne(method.descriptor.requestCompanion, codec)
        case unsupported =>
          ConnectException(
            connectrpc.Code.Unimplemented,
            s"Unsupported compression ${unsupported.coding}. Supported encodings are identity and gzip.",
          ).raiseError[F, method.RequestMessage]
      }
    }
      .adaptError { case error if !error.isInstanceOf[ConnectException] => unaryHandler.decodeError(error) }
      .attempt
      .flatMap {
        case Left(error) =>
          unaryHandler.errorResponse(error).map(_.closeIfHttp1(httpVersion))
        case Right(message) =>
          Async[F].defer(unaryHandler.invokeAndEncode(method, message, entity.headers, codec))
            .handleErrorWith(unaryHandler.errorResponse)
      }

  private def codecFor(request: Request[F], isGet: Boolean): Option[MessageCodec[F]] = {
    val mediaType =
      if (isGet) {
        request.uri.query.multiParams.get("encoding").flatMap(_.headOption).flatMap { value =>
          MediaTypes.parseShort(value).toOption
        }
      } else request.contentType.map(_.mediaType)

    mediaType.flatMap {
      case MediaTypes.`application/json`  => Some(jsonSerdes.codec)
      case MediaTypes.`application/proto` => Some(protoCodec)
      case _                              => None
    }
  }

  private def unsupportedMediaType(request: Request[F], isGet: Boolean): F[Response[F]] = {
    val provided =
      if (isGet) request.uri.query.multiParams.get("encoding").flatMap(_.headOption)
      else request.contentType.map(_.mediaType.show)
    val message = s"Unsupported media type or encoding ${provided.getOrElse("<missing>")}. " +
      "Supported values are application/json, application/proto, json, and proto."
    Async[F].pure(Response[F](UnsupportedMediaType).withEntity(message).closeIfHttp1(request.httpVersion))
  }

}
