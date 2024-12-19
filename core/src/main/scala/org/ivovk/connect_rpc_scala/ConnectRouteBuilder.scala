package org.ivovk.connect_rpc_scala

import cats.Endo
import cats.data.OptionT
import cats.effect.{Async, Resource}
import cats.implicits.*
import io.grpc.{ManagedChannelBuilder, ServerBuilder, ServerServiceDefinition}
import org.http4s.{HttpApp, HttpRoutes, Response, Uri}
import org.ivovk.connect_rpc_scala.connect.{ConnectHandler, ConnectRoutesProvider}
import org.ivovk.connect_rpc_scala.grpc.*
import org.ivovk.connect_rpc_scala.http.*
import org.ivovk.connect_rpc_scala.http.codec.*
import org.ivovk.connect_rpc_scala.transcoding.{TranscodingHandler, TranscodingRoutesProvider, TranscodingUrlMatcher}

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

object ConnectRouteBuilder {

  private val DefaultIncomingHeadersFilter: String => Boolean = name =>
    !name.toLowerCase.startsWith("connection")

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
      incomingHeadersFilter = DefaultIncomingHeadersFilter,
      pathPrefix = Uri.Path.Root,
      executor = ExecutionContext.global,
      waitForShutdown = 5.seconds,
      treatTrailersAsHeaders = true,
    )

}

final class ConnectRouteBuilder[F[_] : Async] private(
  services: Seq[ServerServiceDefinition],
  serverConfigurator: Endo[ServerBuilder[_]],
  channelConfigurator: Endo[ManagedChannelBuilder[_]],
  customJsonSerDeser: Option[JsonSerDeser[F]],
  incomingHeadersFilter: String => Boolean,
  pathPrefix: Uri.Path,
  executor: Executor,
  waitForShutdown: Duration,
  treatTrailersAsHeaders: Boolean,
) {

  private def copy(
    services: Seq[ServerServiceDefinition] = services,
    serverConfigurator: Endo[ServerBuilder[_]] = serverConfigurator,
    channelConfigurator: Endo[ManagedChannelBuilder[_]] = channelConfigurator,
    customJsonSerDeser: Option[JsonSerDeser[F]] = customJsonSerDeser,
    incomingHeadersFilter: String => Boolean = incomingHeadersFilter,
    pathPrefix: Uri.Path = pathPrefix,
    executor: Executor = executor,
    waitForShutdown: Duration = waitForShutdown,
    treatTrailersAsHeaders: Boolean = treatTrailersAsHeaders,
  ): ConnectRouteBuilder[F] =
    new ConnectRouteBuilder(
      services,
      serverConfigurator,
      channelConfigurator,
      customJsonSerDeser,
      incomingHeadersFilter,
      pathPrefix,
      executor,
      waitForShutdown,
      treatTrailersAsHeaders,
    )

  def withServerConfigurator(method: Endo[ServerBuilder[_]]): ConnectRouteBuilder[F] =
    copy(serverConfigurator = method)

  def withChannelConfigurator(method: Endo[ManagedChannelBuilder[_]]): ConnectRouteBuilder[F] =
    copy(channelConfigurator = method)

  def withJsonCodecConfigurator(method: Endo[JsonSerDeserBuilder[F]]): ConnectRouteBuilder[F] =
    copy(customJsonSerDeser = Some(method(JsonSerDeserBuilder[F]()).build))

  def withIncomingHeadersFilter(filter: String => Boolean): ConnectRouteBuilder[F] =
    copy(incomingHeadersFilter = filter)

  def withPathPrefix(path: Uri.Path): ConnectRouteBuilder[F] =
    copy(pathPrefix = path)

  def withExecutor(executor: Executor): ConnectRouteBuilder[F] =
    copy(executor = executor)

  def withWaitForShutdown(duration: Duration): ConnectRouteBuilder[F] =
    copy(waitForShutdown = duration)

  /**
   * When enabled, response trailers are treated as headers (no "trailer-" prefix added).
   *
   * Both `fs2-grpc` and `zio-grpc` support trailing headers only, so enabling this option is a single way to
   * send headers from the server to a client.
   *
   * Enabled by default.
   */
  def withTreatTrailersAsHeaders(enabled: Boolean): ConnectRouteBuilder[F] =
    copy(treatTrailersAsHeaders = enabled)

  /**
   * Use this method only if you want to add additional routes to the server.
   *
   * Otherwise, [[build]] method should be preferred.
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
      val jsonSerDeser  = customJsonSerDeser.getOrElse(JsonSerDeserBuilder[F]().build)
      val codecRegistry = MessageCodecRegistry[F](
        jsonSerDeser.codec,
        ProtoMessageCodec[F](),
      )

      val methodRegistry = MethodRegistry(services)

      val errorHandler = ConnectErrorHandler[F](
        treatTrailersAsHeaders,
      )

      val connectHandler = ConnectHandler(
        channel,
        errorHandler,
        treatTrailersAsHeaders,
        incomingHeadersFilter,
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

      val transcodingHandler = TranscodingHandler(
        channel,
        errorHandler,
        incomingHeadersFilter,
      )

      val transcodingRoutes = TranscodingRoutesProvider(
        transcodingUrlMatcher,
        transcodingHandler,
        jsonSerDeser
      ).routes

      connectRoutes <+> transcodingRoutes
    }
  }

  def build: Resource[F, HttpApp[F]] =
    buildRoutes.map(_.orNotFound)

}
