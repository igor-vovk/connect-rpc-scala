package org.ivovk.connect_rpc_scala.netty

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.Unpooled
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{
  ChannelHandlerContext,
  ChannelInitializer,
  ChannelPipeline,
  SimpleChannelInboundHandler,
}
import io.netty.handler.codec.http.*
import io.netty.handler.timeout.{IdleStateHandler, ReadTimeoutHandler, WriteTimeoutHandler}
import io.netty.util.CharsetUtil
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

object Main extends App {
  val logger = LoggerFactory.getLogger(getClass)

  private val bossGroup = new NioEventLoopGroup(1)
  private val workerGroup =
    new NioEventLoopGroup(Runtime.getRuntime.availableProcessors(), ExecutionContext.global)

  try {
    val bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel]() {
        override def initChannel(channel: SocketChannel): Unit = {
          val pipeline: ChannelPipeline = channel.pipeline()

          pipeline
            // .addLast("logger", new LoggingHandler(LogLevel.INFO))
            .addLast("serverCodec", new HttpServerCodec())
            .addLast("keepAlive", new HttpServerKeepAliveHandler())
            .addLast("aggregator", new HttpObjectAggregator(1048576))
            .addLast("compressor", new HttpContentCompressor())
            .addLast("idleStateHandler", new IdleStateHandler(60, 30, 0))
            .addLast("readTimeoutHandler", new ReadTimeoutHandler(30))
            .addLast("writeTimeoutHandler", new WriteTimeoutHandler(30))
            .addLast("handler", new CustomHttpServerHandler())
        }
      })

    bootstrap.bind(8080)
      .sync()
      .channel().closeFuture.sync()
  } finally {
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
  }
}

class CustomHttpServerHandler extends SimpleChannelInboundHandler[Any] {
  private val responseData = new StringBuilder()

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit =
    ctx.flush()

  override def channelRead0(ctx: ChannelHandlerContext, msg: Any): Unit =
    msg match {
      case request: FullHttpRequest =>
        val requestBody = request.content().toString(CharsetUtil.UTF_8)

        responseData.setLength(0)
        responseData
          .append("\nRequest Body: ")
          .append(requestBody)

        writeResponse(ctx, responseData.toString())
    }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }

  private def writeResponse(ctx: ChannelHandlerContext, message: String): Unit = {
    val bytes = message.getBytes
    val response = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.OK,
      Unpooled.wrappedBuffer(message.getBytes),
    )
    response.headers()
      .set("Content-Type", "text/plain; charset=UTF-8")
      .set("Content-Length", bytes.length)

    ctx.writeAndFlush(response)
  }
}

object RequestUtils {
  def formatParams(request: HttpRequest): String =
    // Implement your logic to format request parameters
    s"Request URI: ${request.uri()}"
}
