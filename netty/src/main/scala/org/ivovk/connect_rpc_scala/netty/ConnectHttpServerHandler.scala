package org.ivovk.connect_rpc_scala.netty

import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.*
import io.netty.util.CharsetUtil
import org.ivovk.connect_rpc_scala.grpc.MethodRegistry

class ConnectHttpServerHandler(
  methodRegistry: MethodRegistry
) extends SimpleChannelInboundHandler[Any] {
  private val responseData = new StringBuilder()

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit =
    ctx.flush()

  override def channelRead0(ctx: ChannelHandlerContext, msg: Any): Unit =
    msg match {
      case request: FullHttpRequest =>
//        val ctype      = request.headers().get(HttpHeaderNames.CONTENT_TYPE)
        val decodedUri = QueryStringDecoder(request.uri)
        val pathParts  = decodedUri.path.split('/').toList

        val grpcMethod = pathParts match
          case service :: method :: Nil =>
            methodRegistry.get(service, method)
          case _ =>
            None
        val requestBody = request.content.toString(CharsetUtil.UTF_8)

        responseData.setLength(0)
        responseData
          .append(s"\nMethod: $grpcMethod")
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
