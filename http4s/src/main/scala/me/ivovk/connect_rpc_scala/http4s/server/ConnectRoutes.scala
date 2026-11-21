package me.ivovk.connect_rpc_scala.http4s.server

import cats.data.OptionT
import cats.effect.Async
import cats.effect.syntax.all.*
import cats.implicits.*
import fs2.{Chunk, Stream}
import me.ivovk.connect_rpc_scala.*
import me.ivovk.connect_rpc_scala.http.MediaTypes
import me.ivovk.connect_rpc_scala.http.codec.*
import me.ivovk.connect_rpc_scala.http.headers.`Connect-Timeout-Ms`
import me.ivovk.connect_rpc_scala.http4s.HttpRoutesSyntax.closeIfHttp1
import org.http4s.Status.{MethodNotAllowed, Ok, UnsupportedMediaType}
import org.http4s.headers.{`Accept-Encoding`, Allow}
import org.http4s.{
  ContentCoding,
  Header,
  Headers,
  HttpRoutes,
  HttpVersion,
  MediaType,
  Method,
  QValue,
  Request,
  Response,
  Uri,
}
import org.typelevel.ci.*

private[http4s] final class ConnectRoutes[F[_]: Async](
  services: Seq[ServiceDefinition[F]],
  codecs: CodecRegistry[F],
  pathPrefix: Uri.Path,
  unaryHandler: UnaryHandler[F],
) {
  private enum RoutedMethod {
    case Unary(method: UnaryMethod[F])
    case ServerStreaming(method: ServerStreamingMethod[F])
  }

  private val methodsByPath: Map[(String, String), RoutedMethod] = {
    val unaryMethods = services
      .flatMap(_.unaryMethods)
      .map { method =>
        (method.descriptor.serviceName -> method.descriptor.methodName) -> RoutedMethod.Unary(method)
      }

    val serverStreamingMethods = services
      .flatMap(_.serverStreamingMethods)
      .map { method =>
        (method.descriptor.serviceName -> method.descriptor.methodName) -> RoutedMethod.ServerStreaming(
          method
        )
      }

    (unaryMethods ++ serverStreamingMethods).toMap
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

        val decodeOptions = DecodeOptions.fromHeaders(request.headers)
        val encoding      = responseEncoding(
          request.headers,
          if isGet then
            request.uri.query.params.get("compression")
              .flatMap(ContentCoding.parse(_).toOption)
              .getOrElse(ContentCoding.identity)
          else decodeOptions.encoding,
        )

        codec.decode(message, decodeOptions)(using method.descriptor.requestCompanion)
          .adaptError {
            case error if !error.isInstanceOf[ConnectException] => unaryHandler.decodeError(error)
          }
          .attempt
          .flatMap {
            case Left(error) =>
              unaryHandler.errorResponse(error, encoding).map(_.closeIfHttp1(request.httpVersion))
            case Right(message) =>
              Async[F].defer(unaryHandler.invokeAndEncode(method, message, request.headers, codec, encoding))
                .handleErrorWith(error => unaryHandler.errorResponse(error, encoding))
          }
          .map(varyByCompression(_, isGet))
    }
  }

  private def responseEncoding(headers: Headers, requestEncoding: => ContentCoding): ContentCoding = {
    val accepted = headers.getWithWarnings[`Accept-Encoding`] match {
      case Some(result) => result.toOption.toList.flatMap(_.values.toList)
      case None         => List(requestEncoding)
    }

    accepted.iterator
      .filter(_.qValue != QValue.Zero)
      .flatMap(coding => Compressor.supportedCompressions.find(_.matches(coding)))
      .nextOption()
      .getOrElse(ContentCoding.identity)
  }

  private def varyByCompression(response: Response[F], isGet: Boolean): Response[F] = {
    val vary = response.headers.get(ci"Vary").toList.flatMap(_.toList)
      .flatMap(_.value.split(',').iterator.map(value => CIString(value.trim)))
    if vary.contains(ci"*") then response
    else {
      val fields =
        if isGet then List(ci"Accept-Encoding") else List(ci"Accept-Encoding", ci"Content-Encoding")
      response.putHeaders(Header.Raw(ci"Vary", (vary ++ fields).distinct.mkString(", ")))
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

          Async[F].defer {
            codec.decodeRequest[method.RequestMessage](request.body, request.headers)(
              using method.descriptor.requestCompanion
            ).compile.toList
          }
            .attempt
            .flatMap {
              case Left(error) =>
                streamingErrorResponse(codec, error).map(_.closeIfHttp1(request.httpVersion))
              case Right(message :: Nil) =>
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
              case Right(messages) =>
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
