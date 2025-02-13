package org.ivovk.connect_rpc_scala.netty

import cats.MonadThrow
import cats.effect.std.Dispatcher
import fs2.{Chunk, Stream}
import io.netty.buffer.{ByteBufUtil, Unpooled}
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http.*
import org.http4s.MediaType
import org.ivovk.connect_rpc_scala.HeaderMapping
import org.ivovk.connect_rpc_scala.grpc.MethodRegistry
import org.ivovk.connect_rpc_scala.http.codec.{MessageCodec, MessageCodecRegistry}
import org.ivovk.connect_rpc_scala.http.{MediaTypes, RequestEntity}
import org.ivovk.connect_rpc_scala.netty.connect.ConnectHandler
import org.slf4j.LoggerFactory

class ConnectHttpServerHandlerFactory[F[_]: MonadThrow](
  dispatcher: Dispatcher[F],
  methodRegistry: MethodRegistry,
  headerMapping: HeaderMapping[HttpHeaders],
  codecRegistry: MessageCodecRegistry[F],
  connectHandler: ConnectHandler[F],
) {
  def createHandler() =
    new HttpServerHandler[F](
      dispatcher = dispatcher,
      methodRegistry = methodRegistry,
      headerMapping = headerMapping,
      codecRegistry = codecRegistry,
      connectHandler = connectHandler,
    )
}

class HttpServerHandler[F[_]: MonadThrow](
  dispatcher: Dispatcher[F],
  methodRegistry: MethodRegistry,
  headerMapping: HeaderMapping[HttpHeaders],
  codecRegistry: MessageCodecRegistry[F],
  connectHandler: ConnectHandler[F],
) extends ChannelInboundHandlerAdapter {

  private val logger = LoggerFactory.getLogger(getClass)

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit =
    ctx.flush()

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit =
    msg match {
      case request: FullHttpRequest =>
        if (logger.isTraceEnabled) {
          logger.trace(s">>> HTTP request: ${request.uri()}")
          logger.trace(s">>> Headers: ${request.headers()}")
        }

        val decodedUri = QueryStringDecoder(request.uri())
        val pathParts  = decodedUri.path.substring(1).split('/').toList

        val grpcMethod = pathParts match {
          case serviceName :: methodName :: Nil =>
            methodRegistry.get(serviceName, methodName)
          case _ =>
            None
        }

        val mediaType = Option(request.headers().get(HttpHeaderNames.CONTENT_TYPE))
          .map(MediaType.unsafeParse)
          .getOrElse(MediaTypes.`application/json`)

        given MessageCodec[F] = codecRegistry.byMediaType(mediaType).get

        val response = grpcMethod match {
          case None =>
            val response = new DefaultFullHttpResponse(
              HttpVersion.HTTP_1_1,
              HttpResponseStatus.BAD_REQUEST,
              Unpooled.wrappedBuffer("Method not found".getBytes),
            )
            response.headers()
              .set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8")
              .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes())

            response
          case Some(methodEntry) =>
            val message =
              if (request.content().hasArray) {
                Stream.chunk(Chunk.array(request.content.array()))
              } else {
                Stream.chunk(Chunk.array(ByteBufUtil.getBytes(request.content)))
              }

            val requestEntity = RequestEntity[F](
              message = message,
              headers = headerMapping.toMetadata(request.headers()),
            )

            // TODO: sync???
            dispatcher.unsafeRunSync(connectHandler.handle(requestEntity, methodEntry))
        }

        ctx.writeAndFlush(response)
    }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }

}
