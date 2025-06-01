package org.ivovk.connect_rpc_scala.netty

import cats.effect.std.Dispatcher
import cats.effect.{Async, Resource}
import cats.implicits.*
import cats.{Endo, Parallel}
import io.grpc.{Channel, ManagedChannelBuilder, ServerBuilder, ServerServiceDefinition}
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelInitializer, MultiThreadIoEventLoopGroup}
import io.netty.handler.codec.http.{HttpObjectAggregator, HttpServerCodec, HttpServerKeepAliveHandler}
import io.netty.handler.logging.{LoggingHandler, LogLevel}
import io.netty.handler.timeout.{IdleStateHandler, ReadTimeoutHandler, WriteTimeoutHandler}
import org.http4s.Uri
import org.ivovk.connect_rpc_scala.grpc.{InProcessChannelBridge, MethodRegistry}
import org.ivovk.connect_rpc_scala.http.codec.{
  JsonSerDeser,
  JsonSerDeserBuilder,
  MessageCodecRegistry,
  ProtoMessageCodec,
}
import org.ivovk.connect_rpc_scala.netty.connect.{ConnectErrorHandler, ConnectHandler}
import org.ivovk.connect_rpc_scala.netty.headers.NettyHeaderMapping
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.ivovk.connect_rpc_scala.http.{HeaderMapping, HeadersFilter}
import org.slf4j.LoggerFactory

import java.net.InetSocketAddress
import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

case class Server(
  address: InetSocketAddress
) {
  def host: String = address.getHostString
  def port: Int    = address.getPort
}

/** Deprecated: Use [[ConnectNettyServerBuilder]] instead. */
object NettyServerBuilder {

  /** Please use [[ConnectNettyServerBuilder.forService]] instead. */
  def forService[F[_]: Async: Parallel](service: ServerServiceDefinition): ConnectNettyServerBuilder[F] =
    forServices(Seq(service))

    /** Please use [[ConnectNettyServerBuilder.forServices]] instead. */
  def forServices[F[_]: Async: Parallel](
    services: Seq[ServerServiceDefinition]
  ): ConnectNettyServerBuilder[F] =
    new ConnectNettyServerBuilder[F](
      services = services,
      serverConfigurator = identity,
      enableLogging = false,
      channelConfigurator = identity,
      customJsonSerDeser = None,
      incomingHeadersFilter = HeaderMapping.DefaultIncomingHeadersFilter,
      outgoingHeadersFilter = HeaderMapping.DefaultOutgoingHeadersFilter,
      pathPrefix = Uri.Path.Root,
      executor = ExecutionContext.global,
      waitForShutdown = 5.seconds,
      treatTrailersAsHeaders = true,
      host = "0.0.0.0",
      port = 0,
    )

}

object ConnectNettyServerBuilder {

  def forService[F[_]: Async: Parallel](service: ServerServiceDefinition): ConnectNettyServerBuilder[F] =
    forServices(Seq(service))

  def forServices[F[_]: Async: Parallel](
    services: Seq[ServerServiceDefinition]
  ): ConnectNettyServerBuilder[F] =
    new ConnectNettyServerBuilder[F](
      services = services,
      serverConfigurator = identity,
      enableLogging = false,
      channelConfigurator = identity,
      customJsonSerDeser = None,
      incomingHeadersFilter = HeaderMapping.DefaultIncomingHeadersFilter,
      outgoingHeadersFilter = HeaderMapping.DefaultOutgoingHeadersFilter,
      pathPrefix = Uri.Path.Root,
      executor = ExecutionContext.global,
      waitForShutdown = 5.seconds,
      treatTrailersAsHeaders = true,
      host = "0.0.0.0",
      port = 0,
    )

}

class ConnectNettyServerBuilder[F[_]: Async: Parallel] private[netty] (
  services: Seq[ServerServiceDefinition],
  serverConfigurator: Endo[ServerBuilder[_]],
  enableLogging: Boolean,
  channelConfigurator: Endo[ManagedChannelBuilder[_]],
  customJsonSerDeser: Option[JsonSerDeser[F]],
  incomingHeadersFilter: HeadersFilter,
  outgoingHeadersFilter: HeadersFilter,
  pathPrefix: Uri.Path,
  executor: Executor,
  waitForShutdown: Duration,
  treatTrailersAsHeaders: Boolean,
  host: String,
  port: Int,
) {

  private val logger = LoggerFactory.getLogger(getClass)

  private def copy(
    services: Seq[ServerServiceDefinition] = services,
    enableLogging: Boolean = enableLogging,
    channelConfigurator: Endo[ManagedChannelBuilder[_]] = channelConfigurator,
    customJsonSerDeser: Option[JsonSerDeser[F]] = customJsonSerDeser,
    incomingHeadersFilter: HeadersFilter = incomingHeadersFilter,
    outgoingHeadersFilter: HeadersFilter = outgoingHeadersFilter,
    pathPrefix: Uri.Path = pathPrefix,
    executor: Executor = executor,
    waitForShutdown: Duration = waitForShutdown,
    treatTrailersAsHeaders: Boolean = treatTrailersAsHeaders,
    host: String = host,
    port: Int = port,
  ): ConnectNettyServerBuilder[F] =
    new ConnectNettyServerBuilder(
      services = services,
      serverConfigurator = serverConfigurator,
      enableLogging = enableLogging,
      channelConfigurator = channelConfigurator,
      customJsonSerDeser = customJsonSerDeser,
      incomingHeadersFilter = incomingHeadersFilter,
      outgoingHeadersFilter = outgoingHeadersFilter,
      pathPrefix = pathPrefix,
      executor = executor,
      waitForShutdown = waitForShutdown,
      treatTrailersAsHeaders = treatTrailersAsHeaders,
      host = host,
      port = port,
    )

  def withChannelConfigurator(method: Endo[ManagedChannelBuilder[_]]): ConnectNettyServerBuilder[F] =
    copy(channelConfigurator = method)

  def withJsonCodecConfigurator(method: Endo[JsonSerDeserBuilder[F]]): ConnectNettyServerBuilder[F] =
    copy(customJsonSerDeser = Some(method(JsonSerDeserBuilder[F]()).build))

  def withPathPrefix(pathPrefix: Uri.Path): ConnectNettyServerBuilder[F] =
    copy(pathPrefix = pathPrefix)

  def withHost(host: String): ConnectNettyServerBuilder[F] =
    copy(host = host)

  def withPort(port: Int): ConnectNettyServerBuilder[F] =
    copy(port = port)

  def build(): Resource[F, Server] =
    for
      channel <- InProcessChannelBridge.create(
        services,
        serverConfigurator,
        channelConfigurator,
        executor,
        waitForShutdown,
      )
      dispatcher <- Dispatcher.parallel[F]
      server     <- mkServer(channel, dispatcher)
    yield server

  private def mkServer(channel: Channel, dispatcher: Dispatcher[F]): Resource[F, Server] = {
    val methodRegistry = MethodRegistry(services)
    val headerMapping  = new NettyHeaderMapping(
      headersFilter = incomingHeadersFilter,
      metadataFilter = outgoingHeadersFilter,
      treatTrailersAsHeaders = treatTrailersAsHeaders,
    )

    val jsonSerDeser  = customJsonSerDeser.getOrElse(JsonSerDeserBuilder[F]().build)
    val codecRegistry = MessageCodecRegistry[F](
      jsonSerDeser.codec,
      ProtoMessageCodec[F](),
    )

    val connectErrorHandler = new ConnectErrorHandler[F](
      headerMapping = headerMapping
    )

    val connectHandler = new ConnectHandler[F](
      channel = channel,
      errorHandler = connectErrorHandler,
      headerMapping = headerMapping,
    )

    val connectHandlerInit = new ConnectHttpHandlerInitializer(
      dispatcher = dispatcher,
      methodRegistry = methodRegistry,
      headerMapping = headerMapping,
      codecRegistry = codecRegistry,
      connectHandler = connectHandler,
      pathPrefix = pathPrefix.segments.map(_.encoded).toList,
    )

    val ioHandlerFactory = NioIoHandler.newFactory()
    val bossGroup        = new MultiThreadIoEventLoopGroup(1, ioHandlerFactory)
    val workerGroup      = new MultiThreadIoEventLoopGroup(1, ioHandlerFactory)

    val bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel]() {
        override def initChannel(channel: SocketChannel): Unit = {
          val pipeline = channel.pipeline()

          pipeline
            .pipeIf(enableLogging)(_.addLast("logger", new LoggingHandler(LogLevel.INFO)))
            .addLast("serverCodec", new HttpServerCodec())
            .addLast("keepAlive", new HttpServerKeepAliveHandler())
            .addLast("aggregator", new HttpObjectAggregator(1 * 1024 * 1024))
            .addLast("idleStateHandler", new IdleStateHandler(60, 30, 0))
            .addLast("readTimeoutHandler", new ReadTimeoutHandler(30))
            .addLast("writeTimeoutHandler", new WriteTimeoutHandler(30))
            .addLast("handler", connectHandlerInit.createHandler())
        }
      })

    val aloc: F[Server] = Async[F].delay {
      val channel = bootstrap.bind(host, port)
        .sync()
        .channel()

      Server(
        address = channel.localAddress() match {
          case inet: InetSocketAddress =>
            inet
          case _ => throw new RuntimeException("Failed to get local address")
        }
      )
    }

    val release: F[Unit] =
      Async[F].delay(logger.trace("Shutting down server")) *>
        (
          NettyFutureAsync[F].fromFuture_(bossGroup.shutdownGracefully()),
          NettyFutureAsync[F].fromFuture_(workerGroup.shutdownGracefully()),
        ).parTupled.void

    Resource.make(aloc)(_ => release)
  }

}
