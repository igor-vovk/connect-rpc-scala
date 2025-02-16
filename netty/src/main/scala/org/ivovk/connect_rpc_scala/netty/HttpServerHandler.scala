package org.ivovk.connect_rpc_scala.netty

import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.implicits.*
import fs2.Stream
import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http.*
import org.ivovk.connect_rpc_scala.HeaderMapping
import org.ivovk.connect_rpc_scala.grpc.MethodRegistry
import org.ivovk.connect_rpc_scala.http.Paths.extractPathSegments
import org.ivovk.connect_rpc_scala.http.codec.{MessageCodec, MessageCodecRegistry}
import org.ivovk.connect_rpc_scala.http.{MediaTypes, RequestEntity}
import org.ivovk.connect_rpc_scala.netty.ByteBufConversions.byteBufToChunk
import org.ivovk.connect_rpc_scala.netty.connect.ConnectHandler
import org.slf4j.LoggerFactory

class ConnectHttpHandlerInitializer[F[_]: Async](
  dispatcher: Dispatcher[F],
  methodRegistry: MethodRegistry,
  headerMapping: HeaderMapping[HttpHeaders],
  codecRegistry: MessageCodecRegistry[F],
  connectHandler: ConnectHandler[F],
  pathPrefix: List[String],
) {
  def createHandler() =
    new HttpServerHandler[F](
      dispatcher = dispatcher,
      methodRegistry = methodRegistry,
      headerMapping = headerMapping,
      codecRegistry = codecRegistry,
      connectHandler = connectHandler,
      pathPrefix = pathPrefix,
    )
}

class HttpServerHandler[F[_]: Async](
  dispatcher: Dispatcher[F],
  methodRegistry: MethodRegistry,
  headerMapping: HeaderMapping[HttpHeaders],
  codecRegistry: MessageCodecRegistry[F],
  connectHandler: ConnectHandler[F],
  pathPrefix: List[String],
) extends ChannelInboundHandlerAdapter,
      NettyFutureAsync[F] {
  import HttpServerHandler.*

  private val logger = LoggerFactory.getLogger(getClass)

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
    super.channelReadComplete(ctx)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit =
    msg match {
      case req: FullHttpRequest =>
        if (logger.isTraceEnabled) {
          logger.trace(s">>> HTTP request: ${req.method()} ${req.uri()}")
          logger.trace(s">>> Headers: ${req.headers()}")
        }

        val decodedUri   = QueryStringDecoder(req.uri())
        val pathSegments = extractPathSegments(decodedUri.rawPath, pathPrefix)

        val maybeGrpcMethod = pathSegments match {
          case Some(serviceName :: methodName :: Nil) =>
            // Temporary support GET-requests for all methods,
            // until https://github.com/scalapb/ScalaPB/pull/1774 is merged
            methodRegistry.get(serviceName, methodName) // .filter(_.descriptor.isSafe || aGetMethod)
          case _ => None
        }

        val responseF = maybeGrpcMethod match {
          // Found a GRPC method, continue processing the request as Connect protocol
          case Some(methodEntry) =>
            val aGetMethod = req.method() == HttpMethod.GET

            val maybeMediaType =
              if aGetMethod then decodedUri.queryParam("encoding").map(MediaTypes.parseShort)
              else Option(req.headers().get(HttpHeaderNames.CONTENT_TYPE)).map(MediaTypes.parse)

            val mediaType = maybeMediaType match {
              case Some(Right(mt)) => mt
              case Some(Left(e)) =>
                sendError(ctx, e.getMessage, HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE)
                return
              case None =>
                sendError(ctx, "Encoding is missing", HttpResponseStatus.BAD_REQUEST)
                return
            }

            given MessageCodec[F] = codecRegistry.byMediaType(mediaType).get

            val message: String | Stream[F, Byte] =
              if aGetMethod then decodedUri.queryParam("message").getOrElse("")
              else Stream.chunk(byteBufToChunk(req.content()))

            val entity = RequestEntity[F](message, headerMapping.toMetadata(req.headers()))

            connectHandler.handle(entity, methodEntry)
          case None =>
            errorResponse("Method not found", HttpResponseStatus.BAD_REQUEST).pure[F]
        }

        dispatcher.unsafeRunAndForget {
          responseF.attempt
            // .flatTap { _ =>
            //  Async[F].delay(req.release())
            // }
            .flatMap { ei =>
              val response = ei match {
                case Right(response) => response
                case Left(e) =>
                  logger.error("Error processing request", e)
                  errorResponse(e.getMessage)
              }

              fromFuture_ {
                if (ctx.channel().isOpen) {
                  ctx.writeAndFlush(response)
                } else {
                  logger.warn("Channel is closed, cannot send response for request {}", req.uri())

                  ctx.channel().newSucceededFuture()
                }
              } *>
                Async[F].delay(ctx.fireChannelReadComplete())
            }
        }
    }

  private def sendError(
    ctx: ChannelHandlerContext,
    message: String,
    status: HttpResponseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR,
  ): Unit = {
    val response = errorResponse(message, status)
    ctx.writeAndFlush(response)
  }

  private def errorResponse(
    message: String,
    httpCode: HttpResponseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR,
  ): FullHttpResponse = {
    val response = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      httpCode,
      Unpooled.wrappedBuffer(message.getBytes),
    )
    response.headers()
      .set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8")
      .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes())

    response
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    logger.error("Fatal exception caught", cause)

    if (ctx.channel.isOpen) {
      val response = errorResponse(cause.getMessage, HttpResponseStatus.INTERNAL_SERVER_ERROR)

      ctx.writeAndFlush(response)
        .addListener(ChannelFutureListener.CLOSE)
    }
  }

}

object HttpServerHandler {

  extension (inline uri: QueryStringDecoder) {
    inline def queryParam(inline name: String): Option[String] =
      Option(uri.parameters.get(name))
        .flatMap { list =>
          if !list.isEmpty then Some(list.get(0)) else None
        }
  }

}
