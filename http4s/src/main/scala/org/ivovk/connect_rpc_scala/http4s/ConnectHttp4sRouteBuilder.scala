package org.ivovk.connect_rpc_scala.http4s

import cats.Endo
import cats.data.OptionT
import cats.effect.{Async, Resource}
import cats.effect.syntax.all.*
import cats.implicits.*
import fs2.Stream
import org.http4s.Status.UnsupportedMediaType
import org.http4s.{
  ContentCoding,
  Header,
  Headers,
  HttpApp,
  HttpRoutes,
  MediaType,
  Method,
  Request,
  Response,
  Status,
  Uri,
}
import org.ivovk.connect_rpc_scala.http.HeaderMapping
import org.ivovk.connect_rpc_scala.http.MediaTypes
import org.ivovk.connect_rpc_scala.http.codec.*
import org.ivovk.connect_rpc_scala.service.MergingBuilder.*
import org.ivovk.connect_rpc_scala.service.*
import org.ivovk.connect_rpc_scala.transcoding.{MatchedRequest, TranscodingUrlMatcher}
import org.typelevel.ci.CIString
import scalapb.GeneratedMessage

import java.nio.charset.Charset
import scala.concurrent.duration.*

object ConnectHttp4sRouteBuilder {

  def forService[F[_]: Async](service: ServiceDefinition[F]): ConnectHttp4sRouteBuilder[F] =
    forServices(Seq(service))

  def forServices[F[_]: Async](
    service: ServiceDefinition[F],
    other: ServiceDefinition[F]*
  ): ConnectHttp4sRouteBuilder[F] =
    forServices(service +: other)

  def forServices[F[_]: Async](services: Seq[ServiceDefinition[F]]): ConnectHttp4sRouteBuilder[F] =
    new ConnectHttp4sRouteBuilder(
      services = services,
      jsonSerdes = JsonSerdesBuilder[F]().build,
      incomingHeadersFilter = HeaderMapping.DefaultIncomingHeadersFilter,
      outgoingHeadersFilter = HeaderMapping.DefaultOutgoingHeadersFilter,
      pathPrefix = Uri.Path.Root,
      treatTrailersAsHeaders = true,
      additionalRoutes = None,
    )
}

final class ConnectHttp4sRouteBuilder[F[_]: Async] private[http4s] (
  services: Seq[ServiceDefinition[F]],
  jsonSerdes: JsonSerdes[F],
  incomingHeadersFilter: String => Boolean,
  outgoingHeadersFilter: String => Boolean,
  pathPrefix: Uri.Path,
  treatTrailersAsHeaders: Boolean,
  additionalRoutes: Option[HttpRoutes[F]],
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

  private val transcodingMatcher = TranscodingUrlMatcher[F](
    services.flatMap(_.methods),
    pathPrefix.segments.map(_.encoded).toList,
  )

  private def copy(
    services: Seq[ServiceDefinition[F]] = services,
    jsonSerdes: JsonSerdes[F] = jsonSerdes,
    incomingHeadersFilter: String => Boolean = incomingHeadersFilter,
    outgoingHeadersFilter: String => Boolean = outgoingHeadersFilter,
    pathPrefix: Uri.Path = pathPrefix,
    treatTrailersAsHeaders: Boolean = treatTrailersAsHeaders,
    additionalRoutes: Option[HttpRoutes[F]] = additionalRoutes,
  ): ConnectHttp4sRouteBuilder[F] =
    new ConnectHttp4sRouteBuilder(
      services,
      jsonSerdes,
      incomingHeadersFilter,
      outgoingHeadersFilter,
      pathPrefix,
      treatTrailersAsHeaders,
      additionalRoutes,
    )

  def withJsonCodecConfigurator(
    configure: Endo[JsonSerdesBuilder[F]]
  ): ConnectHttp4sRouteBuilder[F] =
    copy(jsonSerdes = configure(JsonSerdesBuilder[F]()).build)

  def withIncomingHeadersFilter(filter: String => Boolean): ConnectHttp4sRouteBuilder[F] =
    copy(incomingHeadersFilter = filter)

  def withOutgoingHeadersFilter(filter: String => Boolean): ConnectHttp4sRouteBuilder[F] =
    copy(outgoingHeadersFilter = filter)

  def withPathPrefix(path: Uri.Path): ConnectHttp4sRouteBuilder[F] =
    copy(pathPrefix = path)

  def disableTreatingTrailersAsHeaders: ConnectHttp4sRouteBuilder[F] =
    copy(treatTrailersAsHeaders = false)

  def withAdditionalRoutes(routes: HttpRoutes[F]): ConnectHttp4sRouteBuilder[F] =
    copy(additionalRoutes = Some(routes))

  def build: Resource[F, HttpApp[F]] =
    buildRoutes.map { routes =>
      additionalRoutes.fold(routes)(routes <+> _).orNotFound
    }

  def buildRoutes: Resource[F, HttpRoutes[F]] = Resource.pure {
    HttpRoutes[F] { request =>
      findConnectMethod(request) match {
        case Some(method) => OptionT.liftF(handleConnect(request, method))
        case None         =>
          matchTranscoding(request).fold(OptionT.none[F, Response[F]]) { matched =>
            OptionT.liftF(handleTranscoding(request, matched))
          }
      }
    }
  }

  private def findConnectMethod(request: Request[F]): Option[UnaryMethod[F]] =
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

  private def matchTranscoding(request: Request[F]): Option[MatchedRequest[F]] =
    transcodingMatcher.matchRequest(
      request.method,
      request.uri.path.segments.map(_.encoded).toList,
      request.uri.query.pairs,
    )

  private def handleConnect(request: Request[F], method: UnaryMethod[F]): F[Response[F]] = {
    val isGet = request.method == Method.GET
    codecFor(request, isGet) match {
      case None        => unsupportedMediaType(request, isGet)
      case Some(codec) =>
        val message: String | Stream[F, Byte] =
          if isGet then request.uri.query.multiParams.get("message").flatMap(_.headOption).getOrElse("")
          else request.body

        decodeAndInvoke(
          request,
          method,
          EntityToDecode(message, connectHeaders(request)),
          codec,
        )
    }
  }

  private def handleTranscoding(
    request: Request[F],
    matched: MatchedRequest[F],
  ): F[Response[F]] = {
    val method                                                     = matched.method
    given scalapb.GeneratedMessageCompanion[method.RequestMessage] = method.descriptor.requestCompanion

    val bodyCodec = jsonSerdes.codec.withDecodingJsonTransform(matched.reqBodyTransform)
    val headers   = connectHeaders(request)

    EntityToDecode[F](request.body, headers).as[method.RequestMessage](using summon, bodyCodec)
      .compile.onlyOrError
      .map { bodyMessage =>
        val pathMessage  = jsonSerdes.parser.fromJson[method.RequestMessage](matched.pathJson)
        val queryMessage = jsonSerdes.parser.fromJson[method.RequestMessage](matched.queryJson)
        bodyMessage.merge(pathMessage).merge(queryMessage).build
      }
      .adaptError { case error => decodeError(error) }
      .flatMap(message => invoke(method, message, ConnectRequestContext(headers, timeout(headers))))
      .flatMap(result => encodeResult(result, jsonSerdes.codec))
      .handleErrorWith(error => errorResponse(error, jsonSerdes.codec))
  }

  private def decodeAndInvoke(
    request: Request[F],
    method: UnaryMethod[F],
    entity: EntityToDecode[F],
    codec: MessageCodec[F],
  ): F[Response[F]] = {
    given scalapb.GeneratedMessageCompanion[method.RequestMessage] = method.descriptor.requestCompanion
    val headers                                                    = connectHeaders(request)

    entity.as[method.RequestMessage](using summon, codec)
      .compile.onlyOrError
      .adaptError { case error => decodeError(error) }
      .flatMap(message => invoke(method, message, ConnectRequestContext(headers, timeout(headers))))
      .flatMap(result => encodeResult(result, codec))
      .handleErrorWith(error => errorResponse(error, codec))
  }

  private def invoke(
    method: UnaryMethod[F],
    request: method.RequestMessage,
    ctx: ConnectRequestContext,
  ): F[ConnectResponse[method.ResponseMessage]] = {
    val response = method.invoke(request, ctx)
    ctx.timeout.fold(response) { duration =>
      response.timeoutTo(
        duration,
        ConnectException(connectrpc.Code.DeadlineExceeded, "Request deadline exceeded").raiseError,
      )
    }
  }

  private def decodeError(error: Throwable): ConnectException =
    ConnectException(
      connectrpc.Code.InvalidArgument,
      Option(error.getMessage).getOrElse("Unable to decode the request"),
    ).withCause(error)

  private def encodeResult(
    result: ConnectResponse[? <: GeneratedMessage],
    codec: MessageCodec[F],
  ): F[Response[F]] = {
    val responseHeaders = toHttp4sHeaders(result.headers) ++ toHttp4sTrailers(result.trailers)
    encodeResponse(Status.Ok, responseHeaders, result.message, codec)
  }

  private def errorResponse(error: Throwable, codec: MessageCodec[F]): F[Response[F]] = {
    val exception = error match {
      case connect: ConnectException => connect
      case other                     =>
        ConnectException(
          connectrpc.Code.Internal,
          Option(other.getMessage).getOrElse("Internal error"),
        ).withCause(other)
    }

    val status  = Status.fromInt(httpStatus(exception.code)).fold(throw _, identity)
    val headers = toHttp4sHeaders(exception.headers ++ exception.trailers)
    val body    = connectrpc.Error(
      code = exception.code,
      message = Option(exception.getMessage).filter(_.nonEmpty),
      details = exception.details,
    )
    encodeResponse(status, headers, body, codec)
  }

  private def encodeResponse(
    status: Status,
    headers: Headers,
    message: GeneratedMessage,
    codec: MessageCodec[F],
  ): F[Response[F]] = {
    val entity = codec.encode(
      Stream.emit(message),
      EncodeOptions(Charset.defaultCharset(), ContentCoding.identity),
    )
    Async[F].pure(
      Response[F](
        status = status,
        headers = headers.put(entity.headers.map(Header.ToRaw.keyValuesToRaw).toSeq*),
        body = entity.body,
      )
    )
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

  private def connectHeaders(request: Request[F]): ConnectHeaders =
    ConnectHeaders.create(
      request.headers.headers.iterator
        .map(header => header.name.toString -> header.value)
        .filter((name, _) => incomingHeadersFilter(name))
        .toSeq
    )

  private def timeout(headers: ConnectHeaders): Option[FiniteDuration] =
    headers.get("connect-timeout-ms")
      .flatMap(_.headOption)
      .flatMap(_.toLongOption)
      .map(_.millis)

  private def unsupportedMediaType(request: Request[F], isGet: Boolean): F[Response[F]] = {
    val provided =
      if isGet then request.uri.query.multiParams.get("encoding").flatMap(_.headOption)
      else request.contentType.map(_.mediaType.show)
    val message = s"Unsupported media type or encoding ${provided.getOrElse("<missing>")}. " +
      "Supported values are application/json, application/proto, json, and proto."
    Async[F].pure(Response[F](UnsupportedMediaType).withEntity(message))
  }

  private def toHttp4sHeaders(headers: ConnectHeaders): Headers =
    toHttp4sHeaders(headers, trailing = false)

  private def toHttp4sTrailers(headers: ConnectHeaders): Headers =
    toHttp4sHeaders(headers, trailing = !treatTrailersAsHeaders)

  private def toHttp4sHeaders(headers: ConnectHeaders, trailing: Boolean): Headers = {
    val raw = headers.entries.toSeq.flatMap { case (name, values) =>
      if outgoingHeadersFilter(name) then {
        val outputName = if trailing then s"trailer-$name" else name
        values.map(value => Header.Raw(CIString(outputName), value))
      } else Seq.empty
    }
    new Headers(raw.toList)
  }

  private def httpStatus(code: connectrpc.Code): Int = code match {
    case connectrpc.Code.Canceled           => 499
    case connectrpc.Code.InvalidArgument    => 400
    case connectrpc.Code.DeadlineExceeded   => 504
    case connectrpc.Code.NotFound           => 404
    case connectrpc.Code.AlreadyExists      => 409
    case connectrpc.Code.PermissionDenied   => 403
    case connectrpc.Code.ResourceExhausted  => 429
    case connectrpc.Code.FailedPrecondition => 400
    case connectrpc.Code.Aborted            => 409
    case connectrpc.Code.OutOfRange         => 400
    case connectrpc.Code.Unimplemented      => 501
    case connectrpc.Code.Unavailable        => 503
    case connectrpc.Code.Unauthenticated    => 401
    case _                                  => 500
  }
}
