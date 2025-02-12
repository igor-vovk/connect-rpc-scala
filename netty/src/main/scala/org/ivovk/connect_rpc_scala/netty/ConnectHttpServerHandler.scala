package org.ivovk.connect_rpc_scala.netty

import cats.effect.std.Dispatcher
import fs2.{Chunk, Stream}
import io.grpc.Channel
import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http.*
import io.netty.util.CharsetUtil
import org.ivovk.connect_rpc_scala.HeaderMapping
import org.ivovk.connect_rpc_scala.grpc.MethodRegistry
import org.ivovk.connect_rpc_scala.http.codec.MessageCodecRegistry
import org.ivovk.connect_rpc_scala.http.{MediaTypes, RequestEntity}

class ConnectHttpServerHandlerFactory[F[_]](
  dispatcher: Dispatcher[F],
  channel: Channel,
  methodRegistry: MethodRegistry,
  headerMapping: HeaderMapping[HttpHeaders],
  codecRegistry: MessageCodecRegistry[F],
) {
  def createHandler() =
    new ConnectHttpServerHandler[F](
      dispatcher = dispatcher,
      channel = channel,
      methodRegistry = methodRegistry,
      headerMapping = headerMapping,
      codecRegistry = codecRegistry,
    )
}

class ConnectHttpServerHandler[F[_]](
  dispatcher: Dispatcher[F],
  channel: Channel,
  methodRegistry: MethodRegistry,
  headerMapping: HeaderMapping[HttpHeaders],
  codecRegistry: MessageCodecRegistry[F],
) extends ChannelInboundHandlerAdapter {
  private val responseData = new StringBuilder()

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit =
    ctx.flush()

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit =
    msg match {
      case request: FullHttpRequest =>
//        val ctype      = request.headers().get(HttpHeaderNames.CONTENT_TYPE)
        val decodedUri = QueryStringDecoder(request.uri)
        val pathParts  = decodedUri.path.split('/').toList

        val grpcMethod = pathParts match
          case serviceName :: methodName :: Nil =>
            methodRegistry.get(serviceName, methodName)
          case _ =>
            None

        grpcMethod match {
          case None =>
            writeResponse(ctx, "Method not found")
            return
          case Some(methodEntry) =>
            val requestEntity = RequestEntity[F](
              message = Stream.chunk(Chunk.array(request.content.array())),
              headers = headerMapping.toMetadata(request.headers()),
            )

            for requestMessage <- codecRegistry.byMediaType(MediaTypes.`application/json`).get
                .decode(requestEntity)(using methodEntry.requestMessageCompanion)
            yield ()

            // Just to make it compile
            println(requestMessage)
        }
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
