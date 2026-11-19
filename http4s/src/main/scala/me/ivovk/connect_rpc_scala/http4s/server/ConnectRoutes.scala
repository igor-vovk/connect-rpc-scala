package me.ivovk.connect_rpc_scala.http4s.server

import cats.data.OptionT
import cats.effect.Async
import cats.effect.syntax.all.*
import cats.implicits.*
import fs2.{Chunk, Stream}
import org.http4s.Status.{MethodNotAllowed, Ok, UnsupportedMediaType}
import org.http4s.headers.{Allow, Connection}
import org.http4s.{ContentCoding, HttpRoutes, HttpVersion, MediaType, Method, Request, Response, Uri}
import me.ivovk.connect_rpc_scala.http.MediaTypes
import me.ivovk.connect_rpc_scala.http.codec.*
import me.ivovk.connect_rpc_scala.http.headers.`Connect-Timeout-Ms`
import me.ivovk.connect_rpc_scala.*

object ConnectRoutes {
  // A rejected request may have unread body bytes. Tell HTTP/1 clients that the connection cannot be reused.
  extension [F[_]](response: Response[F]) {
    private[http4s] def closeIfHttp1(version: HttpVersion): Response[F] =
      if (version.major == 1) response.putHeaders(Connection.close) else response
  }
}

private[http4s] final class ConnectRoutes[F[_]: Async](
  services: Seq[ServiceDefinition[F]],
  codecs: CodecRegistry[F],
  pathPrefix: Uri.Path,
  unaryHandler: UnaryHandler[F],
) {
  import ConnectRoutes.*

  private enum RoutedMethod {
    case Unary(method: UnaryMethod[F])
    case ServerStreaming(method: ServerStreamingMethod[F])
  }

  private val methodsByPath: Map[(String, String), RoutedMethod] = {
    val unaryMethods = services.flatMap(service => service.unaryMethods.map(method => (service.name, method)))
    val serverStreamingMethods =
      services.flatMap(service => service.serverStreamingMethods.map(method => (service.name, method)))
    val entries = unaryMethods.map { (serviceName, method) =>
      (serviceName -> method.descriptor.methodName) -> RoutedMethod.Unary(method)
    } ++ serverStreamingMethods.map { (serviceName, method) =>
      (serviceName -> method.descriptor.methodName) -> RoutedMethod.ServerStreaming(method)
    }
    val result = entries.toMap

    require(result.size == entries.size, "Duplicate Connect service or method definition")
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

  private def findMethod(requestPath: Uri.Path): Option[RoutedMethod] = {
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

  private def handle(request: Request[F], method: RoutedMethod): F[Response[F]] =
    method match {
      case RoutedMethod.Unary(unary)               => handleUnary(request, unary)
      case RoutedMethod.ServerStreaming(streaming) => handleServerStreaming(request, streaming)
    }

  private def handleUnary(request: Request[F], method: UnaryMethod[F]): F[Response[F]] = {
    val isGet     = request.method.isSafe
    val mediaType = mediaTypeFor(request, isGet)

    mediaType.toOption.flatMap(codecs.unary) match {
      case None        => unsupportedMediaType(mediaType, request.httpVersion)
      case Some(codec) =>
        val message: String | Stream[F, Byte] =
          if (isGet) {
            request.uri.query.pairs
              .collectFirst { case ("message", value) => value }
              .flatten
              .getOrElse("")
          } else request.body

        val entity = EntityToDecode(message, request.headers)

        Async[F].defer {
          val encoding = message match {
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
          .adaptError {
            case error if !error.isInstanceOf[ConnectException] => unaryHandler.decodeError(error)
          }
          .attempt
          .flatMap {
            case Left(error) =>
              unaryHandler.errorResponse(error).map(_.closeIfHttp1(request.httpVersion))
            case Right(message) =>
              Async[F].defer(unaryHandler.invokeAndEncode(method, message, entity.headers, codec))
                .handleErrorWith(unaryHandler.errorResponse)
          }
    }
  }

  private def handleServerStreaming(
    request: Request[F],
    method: ServerStreamingMethod[F],
  ): F[Response[F]] =
    if request.method != Method.POST then
      Async[F].pure(
        Response[F](MethodNotAllowed).putHeaders(Allow(Method.POST)).closeIfHttp1(request.httpVersion)
      )
    else {
      val mediaType = mediaTypeFor(request)

      mediaType.toOption.flatMap(codecs.streaming) match {
        case None        => unsupportedMediaType(mediaType, request.httpVersion, isStreaming = true)
        case Some(codec) =>

          codec.decodeRequest[method.RequestMessage](request.body, request.headers)(
            using method.descriptor.requestCompanion
          )
            .compile.toList
            .flatMap {
              case message :: Nil =>
                val context =
                  ConnectRequestContext(
                    headers = request.headers.removePayloadHeaders,
                    timeout = request.headers.get[`Connect-Timeout-Ms`].map(_.timeout),
                  )
                val invocation = method.invoke(message, context).allocated
                val acquired   = context.timeout.fold(invocation) { duration =>
                  invocation.timeoutTo(duration, deadlineExceeded.raiseError)
                }
                acquired.attempt.flatMap {
                  case Right((result, release)) =>
                    val timedResult = context.timeout.fold(result) { duration =>
                      result.copy(stream = result.stream.timeoutTo(duration, deadlineExceeded))
                    }
                    val entity = codec.encodeResponse(timedResult)
                    Async[F].pure(
                      Response[F](
                        status = Ok,
                        headers = result.headers.removePayloadHeaders ++ entity.headers,
                        body = entity.body.onFinalize(release),
                      )
                    )
                  case Left(error) =>
                    streamingErrorResponse(codec, error)
                }
              case messages =>
                streamingErrorResponse(
                  codec,
                  ConnectException(
                    connectrpc.Code.Unimplemented,
                    s"Server-streaming method requires one request message, got ${messages.size}",
                  ),
                )
            }
            .handleErrorWith(error => streamingErrorResponse(codec, error))
      }
    }

  private def streamingErrorResponse(
    codec: StreamingMessageCodec[F],
    error: Throwable,
  ): F[Response[F]] = {
    val exception = error match {
      case connect: ConnectException => connect
      case other                     =>
        ConnectException(
          connectrpc.Code.Internal,
          Option(other.getMessage).getOrElse("Internal error"),
        ).withCause(other)
    }
    val entity = codec.encodeResponse(
      ConnectStreamingResponse(ConnectStream.raiseError[F, scalapb.GeneratedMessage](exception))
    )

    entity.body.compile.to(Chunk).map { body =>
      val framed = EncodedEntity.fromChunk(entity.headers, body)
      Response[F](
        status = Ok,
        headers = exception.headers.removePayloadHeaders ++ framed.headers,
        body = framed.body,
      )
    }
  }

  private def mediaTypeFor(request: Request[F], isGetRequest: Boolean = false): Either[String, MediaType] =
    if (isGetRequest) {
      request.uri.query.multiParams.get("encoding").flatMap(_.headOption).toRight("<missing>")
        .flatMap(value => MediaTypes.parseShort(value).leftMap(_ => value))
    } else request.contentType.map(_.mediaType).toRight("<missing>")

  private def unsupportedMediaType(
    mediaType: Either[String, MediaType],
    httpVersion: HttpVersion,
    isStreaming: Boolean = false,
  ): F[Response[F]] = {
    val provided = mediaType.fold(identity, _.show)
    val message  =
      if (isStreaming)
        s"Unsupported streaming media type $provided. " +
          "Supported values are application/connect+json and application/connect+proto."
      else
        s"Unsupported media type or encoding $provided. " +
          "Supported values are application/json, application/proto, json, and proto."

    Async[F].pure(Response[F](UnsupportedMediaType).withEntity(message).closeIfHttp1(httpVersion))
  }

  private def deadlineExceeded: ConnectException =
    ConnectException(connectrpc.Code.DeadlineExceeded, "Request deadline exceeded")
}
