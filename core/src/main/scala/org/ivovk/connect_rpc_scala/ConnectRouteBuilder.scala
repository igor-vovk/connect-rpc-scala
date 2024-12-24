package org.ivovk.connect_rpc_scala

import cats.Endo
import cats.data.OptionT
import cats.effect.{Async, Resource}
import cats.implicits.*
import io.grpc.{ManagedChannelBuilder, ServerBuilder, ServerServiceDefinition}
import org.http4s.{HttpApp, HttpRoutes, Uri}
import org.ivovk.connect_rpc_scala.connect.{ConnectErrorHandler, ConnectHandler, ConnectRoutesProvider}
import org.ivovk.connect_rpc_scala.grpc.*
import org.ivovk.connect_rpc_scala.http.*
import org.ivovk.connect_rpc_scala.http.codec.*
import org.ivovk.connect_rpc_scala.transcoding.{TranscodingHandler, TranscodingRoutesProvider, TranscodingUrlMatcher}

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

object ConnectRouteBuilder {

  def forService[F[_] : Async](service: ServerServiceDefinition): ConnectRouteBuilder[F] =
    forServices(Seq(service))

  def forServices[F[_] : Async](service: ServerServiceDefinition, other: ServerServiceDefinition*): ConnectRouteBuilder[F] =
    forServices(service +: other)

  def forServices[F[_] : Async](services: Seq[ServerServiceDefinition]): ConnectRouteBuilder[F] =
    new ConnectRouteBuilder(
      services = services,
      serverConfigurator = identity,
      channelConfigurator = identity,
      customJsonSerDeser = None,
      incomingHeadersFilter = HeaderMapping.DefaultIncomingHeadersFilter,
      outgoingHeadersFilter = HeaderMapping.DefaultOutgoingHeadersFilter,
      pathPrefix = Uri.Path.Root,
      executor = ExecutionContext.global,
      waitForShutdown = 5.seconds,
      treatTrailersAsHeaders = true,
      transcodingErrorHandler = None,
    )

}

final class ConnectRouteBuilder[F[_] : Async] private(
  services: Seq[ServerServiceDefinition],
  serverConfigurator: Endo[ServerBuilder[_]],
  channelConfigurator: Endo[ManagedChannelBuilder[_]],
  customJsonSerDeser: Option[JsonSerDeser[F]],
  incomingHeadersFilter: HeadersFilter,
  outgoingHeadersFilter: HeadersFilter,
  pathPrefix: Uri.Path,
  executor: Executor,
  waitForShutdown: Duration,
  treatTrailersAsHeaders: Boolean,
  transcodingErrorHandler: Option[ErrorHandler[F]],
) {

  private def copy(
    services: Seq[ServerServiceDefinition] = services,
    serverConfigurator: Endo[ServerBuilder[_]] = serverConfigurator,
    channelConfigurator: Endo[ManagedChannelBuilder[_]] = channelConfigurator,
    customJsonSerDeser: Option[JsonSerDeser[F]] = customJsonSerDeser,
    incomingHeadersFilter: HeadersFilter = incomingHeadersFilter,
    outgoingHeadersFilter: HeadersFilter = outgoingHeadersFilter,
    pathPrefix: Uri.Path = pathPrefix,
    executor: Executor = executor,
    waitForShutdown: Duration = waitForShutdown,
    treatTrailersAsHeaders: Boolean = treatTrailersAsHeaders,
    transcodingErrorHandler: Option[ErrorHandler[F]] = transcodingErrorHandler,
  ): ConnectRouteBuilder[F] =
    new ConnectRouteBuilder(
      services,
      serverConfigurator,
      channelConfigurator,
      customJsonSerDeser,
      incomingHeadersFilter,
      outgoingHeadersFilter,
      pathPrefix,
      executor,
      waitForShutdown,
      treatTrailersAsHeaders,
      transcodingErrorHandler,
    )

  def withServerConfigurator(method: Endo[ServerBuilder[_]]): ConnectRouteBuilder[F] =
    copy(serverConfigurator = method)

  def withChannelConfigurator(method: Endo[ManagedChannelBuilder[_]]): ConnectRouteBuilder[F] =
    copy(channelConfigurator = method)

  def withJsonCodecConfigurator(method: Endo[JsonSerDeserBuilder[F]]): ConnectRouteBuilder[F] =
    copy(customJsonSerDeser = Some(method(JsonSerDeserBuilder[F]()).build))

  /**
   * Filter for incoming headers.
   *
   * By default, headers with "connection" prefix are filtered out (GRPC requirement).
   */
  def withIncomingHeadersFilter(filter: String => Boolean): ConnectRouteBuilder[F] =
    copy(incomingHeadersFilter = filter)

  /**
   * Filter for outgoing headers.
   *
   * By default, headers with "grpc-" prefix are filtered out.
   */
  def withOutgoingHeadersFilter(filter: String => Boolean): ConnectRouteBuilder[F] =
    copy(outgoingHeadersFilter = filter)

  /**
   * Prefix for all routes created by this builder.
   *
   * "/" by default.
   */
  def withPathPrefix(path: Uri.Path): ConnectRouteBuilder[F] =
    copy(pathPrefix = path)

  def withExecutor(executor: Executor): ConnectRouteBuilder[F] =
    copy(executor = executor)

  /**
   * Amount of time to wait while GRPC server finishes processing requests that are in progress.
   */
  def withWaitForShutdown(duration: Duration): ConnectRouteBuilder[F] =
    copy(waitForShutdown = duration)

  /**
   * By default, response trailers are treated as headers (no "trailer-" prefix added).
   *
   * Both `fs2-grpc` and `zio-grpc` support only trailing headers,
   * so having this option enabled is a single way to send headers from the server to a client.
   */
  def disableTreatingTrailersAsHeaders: ConnectRouteBuilder[F] =
    copy(treatTrailersAsHeaders = false)

  def withTranscodingErrorHandler(handler: ErrorHandler[F]): ConnectRouteBuilder[F] =
    copy(transcodingErrorHandler = Some(handler))

  /**
   * Builds a complete HTTP app with all routes.
   */
  def build: Resource[F, HttpApp[F]] =
    buildRoutes.map(_.orNotFound)

  /**
   * This method should be used only when additional routes are added to the server.
   *
   * Otherwise, [[build]] method is preferred.
   */
  def buildRoutes: Resource[F, HttpRoutes[F]] = {
    for
      channel <- InProcessChannelBridge.create(
        services,
        serverConfigurator,
        channelConfigurator,
        executor,
        waitForShutdown,
      )
    yield {
      val headerMapping = HeaderMapping(
        incomingHeadersFilter,
        outgoingHeadersFilter,
        treatTrailersAsHeaders,
      )

      val jsonSerDeser  = customJsonSerDeser.getOrElse(JsonSerDeserBuilder[F]().build)
      val codecRegistry = MessageCodecRegistry[F](
        jsonSerDeser.codec,
        ProtoMessageCodec[F](),
      )

      val methodRegistry = MethodRegistry(services)

      val connectErrorHandler = ConnectErrorHandler[F](
        headerMapping,
      )

      val connectHandler = ConnectHandler[F](
        channel,
        connectErrorHandler,
        headerMapping,
      )

      val connectRoutes = ConnectRoutesProvider[F](
        pathPrefix,
        methodRegistry,
        codecRegistry,
        connectHandler,
      ).routes

      val transcodingUrlMatcher = TranscodingUrlMatcher[F](
        methodRegistry.all,
        pathPrefix,
      )

      val transcodingHandler = TranscodingHandler[F](
        channel,
        transcodingErrorHandler.getOrElse(connectErrorHandler),
        headerMapping,
      )

      val transcodingRoutes = TranscodingRoutesProvider[F](
        transcodingUrlMatcher,
        transcodingHandler,
        jsonSerDeser
      ).routes

      connectRoutes <+> transcodingRoutes
    }
  }

}
