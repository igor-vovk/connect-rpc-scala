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
import org.ivovk.connect_rpc_scala.http4s.connect.{ConnectErrorHandler, ConnectHandler, ConnectRoutesProvider}
import org.ivovk.connect_rpc_scala.http4s.transcoding.{TranscodingHandler, TranscodingRoutesProvider}
import org.ivovk.connect_rpc_scala.transcoding.TranscodingUrlMatcher
import org.ivovk.connect_rpc_scala.{HeaderMapping, HeadersFilter}

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

object Http4sRouteBuilder {

  def forService[F[_]: Async](service: ServerServiceDefinition): Http4sRouteBuilder[F] =
    forServices(Seq(service))

  def forServices[F[_]: Async](
    service: ServerServiceDefinition,
    other: ServerServiceDefinition*
  ): Http4sRouteBuilder[F] =
    forServices(service +: other)

  def forServices[F[_]: Async](services: Seq[ServerServiceDefinition]): Http4sRouteBuilder[F] =
    new Http4sRouteBuilder(
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

final class Http4sRouteBuilder[F[_]: Async] private (
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
  ): Http4sRouteBuilder[F] =
    new Http4sRouteBuilder(
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

  def withServerConfigurator(method: Endo[ServerBuilder[_]]): Http4sRouteBuilder[F] =
    copy(serverConfigurator = method)

  def withChannelConfigurator(method: Endo[ManagedChannelBuilder[_]]): Http4sRouteBuilder[F] =
    copy(channelConfigurator = method)

  def withJsonCodecConfigurator(method: Endo[JsonSerDeserBuilder[F]]): Http4sRouteBuilder[F] =
    copy(customJsonSerDeser = Some(method(JsonSerDeserBuilder[F]()).build))

  /**
   * Filter for incoming headers.
   *
   * By default, headers with "connection" prefix are filtered out (GRPC requirement).
   */
  def withIncomingHeadersFilter(filter: String => Boolean): Http4sRouteBuilder[F] =
    copy(incomingHeadersFilter = filter)

  /**
   * Filter for outgoing headers.
   *
   * By default, headers with "grpc-" prefix are filtered out.
   */
  def withOutgoingHeadersFilter(filter: String => Boolean): Http4sRouteBuilder[F] =
    copy(outgoingHeadersFilter = filter)

  /**
   * Prefix for all routes created by this builder.
   *
   * "/" by default.
   */
  def withPathPrefix(path: Uri.Path): Http4sRouteBuilder[F] =
    copy(pathPrefix = path)

  def withExecutor(executor: Executor): Http4sRouteBuilder[F] =
    copy(executor = executor)

  /**
   * Amount of time to wait while GRPC server finishes processing requests that are in progress.
   */
  def withWaitForShutdown(duration: Duration): Http4sRouteBuilder[F] =
    copy(waitForShutdown = duration)

  /**
   * By default, response trailers are treated as headers (no "trailer-" prefix added).
   *
   * Both `fs2-grpc` and `zio-grpc` support only trailing headers, so having this option enabled is a single
   * way to send headers from the server to a client.
   */
  def disableTreatingTrailersAsHeaders: Http4sRouteBuilder[F] =
    copy(treatTrailersAsHeaders = false)

  def withTranscodingErrorHandler(handler: ErrorHandler[F]): Http4sRouteBuilder[F] =
    copy(transcodingErrorHandler = Some(handler))

  /**
   * Builds a complete HTTP app with all routes.
   */
  def build: Resource[F, HttpApp[F]] =
    buildRoutes.map(_.all.orNotFound)

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

      val jsonSerDeser = customJsonSerDeser.getOrElse(JsonSerDeserBuilder[F]().build)
      val codecRegistry = MessageCodecRegistry[F](
        jsonSerDeser.codec,
        ProtoMessageCodec[F](),
      )

      val methodRegistry = MethodRegistry(services)

      val connectErrorHandler = ConnectErrorHandler[F](
        headerMapping
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
        headerMapping,
        connectHandler,
      ).routes

      val transcodingUrlMatcher = TranscodingUrlMatcher[F](
        methodRegistry.all,
        pathPrefix.segments.map(_.encoded).toList,
      )

      val transcodingHandler = TranscodingHandler[F](
        channel,
        transcodingErrorHandler.getOrElse(connectErrorHandler),
        headerMapping,
      )

      val transcodingRoutes = TranscodingRoutesProvider[F](
        transcodingUrlMatcher,
        transcodingHandler,
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
