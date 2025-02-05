package org.ivovk.connect_rpc_scala.netty

import cats.effect.{Resource, Sync}
import io.grpc.ServerServiceDefinition
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
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*

import java.net.InetSocketAddress
import scala.concurrent.ExecutionContext

case class Server(address: InetSocketAddress)

class NettyServerBuilder[F[_]: Sync](
  services: Seq[ServerServiceDefinition],
  enableLogging: Boolean = false,
  host: String = "0.0.0.0",
  port: Int = 0,
) {

//  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def build(): Resource[F, Server] = {
    val methodRegistry = MethodRegistry(services)

    val bossGroup = new NioEventLoopGroup(1)
    val workerGroup =
      new NioEventLoopGroup(Runtime.getRuntime.availableProcessors(), ExecutionContext.global)

    val bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel]() {
        override def initChannel(channel: SocketChannel): Unit = {
          val pipeline = channel.pipeline()

          val connectHttpServerHandler = new ConnectHttpServerHandler(methodRegistry)

          pipeline
            .pipeIf(enableLogging)(_.addLast("logger", LoggingHandler(LogLevel.INFO)))
            .addLast("serverCodec", HttpServerCodec())
            .addLast("keepAlive", HttpServerKeepAliveHandler())
            .addLast("aggregator", HttpObjectAggregator(1048576))
            .addLast("compressor", HttpContentCompressor())
            .addLast("idleStateHandler", new IdleStateHandler(60, 30, 0))
            .addLast("readTimeoutHandler", new ReadTimeoutHandler(30))
            .addLast("writeTimeoutHandler", new WriteTimeoutHandler(30))
            .addLast("handler", connectHttpServerHandler)
        }
      })

    val aloc: F[Server] = Sync[F].delay {
      val channel = bootstrap.bind(host, port)
        .sync()
        .channel()

      Server(channel.localAddress().asInstanceOf[InetSocketAddress])
    }

    val release: F[Unit] = Sync[F].delay {
      bossGroup.shutdownGracefully()
      workerGroup.shutdownGracefully()
    }

    Resource.make(aloc)(_ => release)
  }

}
