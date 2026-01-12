package org.ivovk.connect_rpc_scala.http4s

import cats.data.OptionT
import cats.effect.{Async, Resource}
import cats.implicits.*
import cats.{Endo, Monad}
import io.grpc.{ManagedChannelBuilder, ServerBuilder, ServerServiceDefinition}
import org.http4s.{HttpApp, HttpRoutes, Uri}
import org.ivovk.connect_rpc_scala.grpc.*
import org.ivovk.connect_rpc_scala.http.*
import org.ivovk.connect_rpc_scala.http.codec.*
import org.ivovk.connect_rpc_scala.http4s.Conversions.http4sPathToConnectRpcPath
import org.ivovk.connect_rpc_scala.http4s.connect.{ConnectRoutesProvider, ConnectServerHandler}
import org.ivovk.connect_rpc_scala.http4s.transcoding.{
  DefaultTranscodingErrorHandler,
  TranscodingRoutesProvider,
  TranscodingServerHandler,
}
import org.ivovk.connect_rpc_scala.transcoding.TranscodingUrlMatcher

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

@deprecated("Use ConnectHttp4sRouteBuilder", "0.4.1")
object Http4sRouteBuilder {

  /** Please use [[ConnectHttp4sRouteBuilder.forService]] instead. */
  def forService[F[_]: Async](service: ServerServiceDefinition): ConnectHttp4sRouteBuilder[F] =
    forServices(Seq(service))

  /** Please use [[ConnectHttp4sRouteBuilder.forServices]] instead. */
  def forServices[F[_]: Async](
    service: ServerServiceDefinition,
    other: ServerServiceDefinition*
  ): ConnectHttp4sRouteBuilder[F] =
    forServices(service +: other)

  /** Please use [[ConnectHttp4sRouteBuilder.forServices]] instead. */
  def forServices[F[_]: Async](services: Seq[ServerServiceDefinition]): ConnectHttp4sRouteBuilder[F] =
    new ConnectHttp4sRouteBuilder(
      services = services,
      serverConfigurator = identity,
      channelConfigurator = identity,
      customJsonSerdes = None,
      incomingHeadersFilter = HeaderMapping.DefaultIncomingHeadersFilter,
      outgoingHeadersFilter = HeaderMapping.DefaultOutgoingHeadersFilter,
      pathPrefix = Uri.Path.Root,
      executor = ExecutionContext.global,
      waitForShutdown = 5.seconds,
      treatTrailersAsHeaders = true,
      transcodingErrorHandler = None,
      additionalRoutes = None,
    )

}

object ConnectHttp4sRouteBuilder {

  def forService[F[_]: Async](service: ServerServiceDefinition): ConnectHttp4sRouteBuilder[F] =
    forServices(Seq(service))

  def forServices[F[_]: Async](
    service: ServerServiceDefinition,
    other: ServerServiceDefinition*
  ): ConnectHttp4sRouteBuilder[F] =
    forServices(service +: other)

  def forServices[F[_]: Async](services: Seq[ServerServiceDefinition]): ConnectHttp4sRouteBuilder[F] =
    new ConnectHttp4sRouteBuilder(
      services = services,
      serverConfigurator = identity,
      channelConfigurator = identity,
      customJsonSerdes = None,
      incomingHeadersFilter = HeaderMapping.DefaultIncomingHeadersFilter,
      outgoingHeadersFilter = HeaderMapping.DefaultOutgoingHeadersFilter,
      pathPrefix = Uri.Path.Root,
      executor = ExecutionContext.global,
      waitForShutdown = 5.seconds,
      treatTrailersAsHeaders = true,
      transcodingErrorHandler = None,
      additionalRoutes = None,
    )

}

final class ConnectHttp4sRouteBuilder[F[_]: Async] private[http4s] (
  services: Seq[ServerServiceDefinition],
  serverConfigurator: Endo[ServerBuilder[_]],
  channelConfigurator: Endo[ManagedChannelBuilder[_]],
  customJsonSerdes: Option[JsonSerdes[F]],
  incomingHeadersFilter: HeadersFilter,
  outgoingHeadersFilter: HeadersFilter,
  pathPrefix: Uri.Path,
  executor: Executor,
  waitForShutdown: Duration,
  treatTrailersAsHeaders: Boolean,
  transcodingErrorHandler: Option[ErrorHandler[F]],
  additionalRoutes: Option[HttpRoutes[F]],
) {

  private def copy(
    services: Seq[ServerServiceDefinition] = services,
    serverConfigurator: Endo[ServerBuilder[_]] = serverConfigurator,
    channelConfigurator: Endo[ManagedChannelBuilder[_]] = channelConfigurator,
    customJsonSerdes: Option[JsonSerdes[F]] = customJsonSerdes,
    incomingHeadersFilter: HeadersFilter = incomingHeadersFilter,
    outgoingHeadersFilter: HeadersFilter = outgoingHeadersFilter,
    pathPrefix: Uri.Path = pathPrefix,
    executor: Executor = executor,
    waitForShutdown: Duration = waitForShutdown,
    treatTrailersAsHeaders: Boolean = treatTrailersAsHeaders,
    transcodingErrorHandler: Option[ErrorHandler[F]] = transcodingErrorHandler,
    additionalRoutes: Option[HttpRoutes[F]] = additionalRoutes,
  ): ConnectHttp4sRouteBuilder[F] =
    new ConnectHttp4sRouteBuilder(
      services,
      serverConfigurator,
      channelConfigurator,
      customJsonSerdes,
      incomingHeadersFilter,
      outgoingHeadersFilter,
      pathPrefix,
      executor,
      waitForShutdown,
      treatTrailersAsHeaders,
      transcodingErrorHandler,
      additionalRoutes,
    )

  def withServerConfigurator(method: Endo[ServerBuilder[_]]): ConnectHttp4sRouteBuilder[F] =
    copy(serverConfigurator = method)

  def withChannelConfigurator(method: Endo[ManagedChannelBuilder[_]]): ConnectHttp4sRouteBuilder[F] =
    copy(channelConfigurator = method)

  def withJsonCodecConfigurator(method: Endo[JsonSerdesBuilder[F]]): ConnectHttp4sRouteBuilder[F] =
    copy(customJsonSerdes = Some(method(JsonSerdesBuilder[F]()).build))

  /**
   * Filter for incoming headers.
   *
   * By default, headers with "connection" prefix are filtered out (GRPC requirement).
   */
  def withIncomingHeadersFilter(filter: String => Boolean): ConnectHttp4sRouteBuilder[F] =
    copy(incomingHeadersFilter = filter)

  /**
   * Filter for outgoing headers.
   *
   * By default, headers with "grpc-" prefix are filtered out.
   */
  def withOutgoingHeadersFilter(filter: String => Boolean): ConnectHttp4sRouteBuilder[F] =
    copy(outgoingHeadersFilter = filter)

  /**
   * Prefix for all routes created by this builder.
   *
   * "/" by default.
   */
  def withPathPrefix(path: Uri.Path): ConnectHttp4sRouteBuilder[F] =
    copy(pathPrefix = path)

  def withExecutor(executor: Executor): ConnectHttp4sRouteBuilder[F] =
    copy(executor = executor)

  /**
   * Amount of time to wait while GRPC server finishes processing requests that are in progress.
   */
  def withWaitForShutdown(duration: Duration): ConnectHttp4sRouteBuilder[F] =
    copy(waitForShutdown = duration)

  /**
   * By default, response trailers are treated as headers (no "trailer-" prefix added).
   *
   * Both `fs2-grpc` and `zio-grpc` support only trailing headers, so having this option enabled is a single
   * way to send headers from the server to a client.
   */
  def disableTreatingTrailersAsHeaders: ConnectHttp4sRouteBuilder[F] =
    copy(treatTrailersAsHeaders = false)

  def withTranscodingErrorHandler(handler: ErrorHandler[F]): ConnectHttp4sRouteBuilder[F] =
    copy(transcodingErrorHandler = Some(handler))

  /**
   * Add your own additional routes to the Connect HTTP app.
   */
  def withAdditionalRoutes(routes: HttpRoutes[F]): ConnectHttp4sRouteBuilder[F] =
    copy(additionalRoutes = Some(routes))

  /**
   * Builds a complete HTTP app with all routes.
   */
  def build: Resource[F, HttpApp[F]] =
    for routes <- buildRoutes
    yield Seq.concat(routes.all.some, additionalRoutes).reduce(_ <+> _).orNotFound

  /**
   * Use this method if you want to add additional routes and/or http4s middleware.
   *
   * Otherwise, [[build]] method is preferred.
   */
  def buildRoutes: Resource[F, Routes[F]] =
    for channel <- InProcessChannelBridge.create(
        services,
        serverConfigurator,
        channelConfigurator,
        executor,
        waitForShutdown,
      )
    yield {
      val headerMapping = Http4sHeaderMapping(
        incomingHeadersFilter,
        outgoingHeadersFilter,
        treatTrailersAsHeaders,
      )

      val jsonSerDeser  = customJsonSerdes.getOrElse(JsonSerdesBuilder[F]().build)
      val codecRegistry = MessageCodecRegistry[F](
        jsonSerDeser.codec,
        jsonSerDeser.streamingCodec,
        ProtoMessageCodec[F](),
      )

      val methodRegistry = MethodRegistry(services)

      val connectServerHandler = ConnectServerHandler[F](
        channel,
        headerMapping,
      )

      val connectRoutes = ConnectRoutesProvider[F](
        http4sPathToConnectRpcPath(pathPrefix),
        methodRegistry,
        codecRegistry,
        headerMapping,
        connectServerHandler,
      ).routes

      val transcodingUrlMatcher = TranscodingUrlMatcher[F](
        methodRegistry.all,
        http4sPathToConnectRpcPath(pathPrefix),
      )

      val transcodingServerHandler = TranscodingServerHandler[F](
        channel,
        transcodingErrorHandler.getOrElse(DefaultTranscodingErrorHandler()),
        headerMapping,
      )

      val transcodingRoutes = TranscodingRoutesProvider[F](
        transcodingUrlMatcher,
        transcodingServerHandler,
        headerMapping,
        jsonSerDeser,
      ).routes

      Routes(connectRoutes, transcodingRoutes)
    }

}

case class Routes[F[_]: Monad](
  connect: HttpRoutes[F],
  transcoding: HttpRoutes[F],
) {
  def all: HttpRoutes[F] = connect <+> transcoding
}
