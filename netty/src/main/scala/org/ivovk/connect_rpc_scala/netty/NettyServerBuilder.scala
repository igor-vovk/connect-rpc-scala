package org.ivovk.connect_rpc_scala.netty

import cats.Endo
import cats.effect.{Resource, Sync}
import io.grpc.{ManagedChannelBuilder, ServerServiceDefinition}
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.{
  HttpContentCompressor,
  HttpObjectAggregator,
  HttpServerCodec,
  HttpServerKeepAliveHandler,
}
import io.netty.handler.logging.{LoggingHandler, LogLevel}
import io.netty.handler.timeout.{IdleStateHandler, ReadTimeoutHandler, WriteTimeoutHandler}
import org.ivovk.connect_rpc_scala.grpc.MethodRegistry
import org.ivovk.connect_rpc_scala.http.codec.{JsonSerDeser, JsonSerDeserBuilder}
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*

import java.net.InetSocketAddress
import scala.concurrent.ExecutionContext

case class Server(
  address: InetSocketAddress
)

object NettyServerBuilder {

  def forServices[F[_]: Sync](services: Seq[ServerServiceDefinition]): NettyServerBuilder[F] =
    new NettyServerBuilder[F](
      services = services
    )

}

class NettyServerBuilder[F[_]: Sync] private (
  services: Seq[ServerServiceDefinition],
  enableLogging: Boolean = false,
  channelConfigurator: Endo[ManagedChannelBuilder[_]] = identity,
  customJsonSerDeser: Option[JsonSerDeser[F]] = None,
  host: String = "0.0.0.0",
  port: Int = 0,
) {

//  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private def copy(
    services: Seq[ServerServiceDefinition] = services,
    enableLogging: Boolean = enableLogging,
    channelConfigurator: Endo[ManagedChannelBuilder[_]] = channelConfigurator,
    customJsonSerDeser: Option[JsonSerDeser[F]] = customJsonSerDeser,
    host: String = host,
    port: Int = port,
  ): NettyServerBuilder[F] =
    new NettyServerBuilder(
      services = services,
      enableLogging = enableLogging,
      channelConfigurator = channelConfigurator,
      customJsonSerDeser = customJsonSerDeser,
      host = host,
      port = port,
    )

  def withChannelConfigurator(method: Endo[ManagedChannelBuilder[_]]): NettyServerBuilder[F] =
    copy(channelConfigurator = method)

  def withJsonCodecConfigurator(method: Endo[JsonSerDeserBuilder[F]]): NettyServerBuilder[F] =
    copy(customJsonSerDeser = Some(method(JsonSerDeserBuilder[F]()).build))

  def withHost(host: String): NettyServerBuilder[F] =
    copy(host = host)

  def withPort(port: Int): NettyServerBuilder[F] =
    copy(port = port)

  def build(): Resource[F, Server] = {
    val methodRegistry = MethodRegistry(services)

    val bossGroup = new NioEventLoopGroup(1)
    val workerGroup = new NioEventLoopGroup(
      Runtime.getRuntime.availableProcessors(),
      ExecutionContext.global,
    )

    val bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel]() {
        override def initChannel(channel: SocketChannel): Unit = {
          val pipeline       = channel.pipeline()
          val connectHandler = ConnectHttpServerHandler(methodRegistry)

          pipeline
            .pipeIf(enableLogging)(_.addLast("logger", LoggingHandler(LogLevel.INFO)))
            .addLast("serverCodec", HttpServerCodec())
            .addLast("keepAlive", HttpServerKeepAliveHandler())
            .addLast("aggregator", HttpObjectAggregator(1048576))
            .addLast("compressor", HttpContentCompressor())
            .addLast("idleStateHandler", IdleStateHandler(60, 30, 0))
            .addLast("readTimeoutHandler", ReadTimeoutHandler(30))
            .addLast("writeTimeoutHandler", WriteTimeoutHandler(30))
            .addLast("handler", connectHandler)
        }
      })

    val aloc: F[Server] = Sync[F].delay {
      val channel = bootstrap.bind(host, port)
        .sync()
        .channel()

      Server(
        address = channel.localAddress().asInstanceOf[InetSocketAddress]
      )
    }

    val release: F[Unit] = Sync[F].delay {
      bossGroup.shutdownGracefully()
      workerGroup.shutdownGracefully()
    }

    Resource.make(aloc)(_ => release)
  }

}
