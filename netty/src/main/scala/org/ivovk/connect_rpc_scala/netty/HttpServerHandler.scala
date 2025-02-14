package org.ivovk.connect_rpc_scala.netty

import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.implicits.*
import fs2.{Chunk, Stream}
import io.netty.buffer.{ByteBufUtil, Unpooled}
import io.netty.channel.{
  ChannelFuture,
  ChannelHandlerContext,
  ChannelInboundHandlerAdapter,
  DefaultChannelPromise,
}
import io.netty.handler.codec.http.*
import io.netty.util.concurrent.GenericFutureListener
import org.ivovk.connect_rpc_scala.HeaderMapping
import org.ivovk.connect_rpc_scala.grpc.MethodRegistry
import org.ivovk.connect_rpc_scala.http.codec.{MessageCodec, MessageCodecRegistry}
import org.ivovk.connect_rpc_scala.http.{MediaTypes, RequestEntity}
import org.ivovk.connect_rpc_scala.netty.connect.ConnectHandler
import org.slf4j.LoggerFactory

class ConnectHttpServerHandlerFactory[F[_]: Async](
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

class HttpServerHandler[F[_]: Async](
  dispatcher: Dispatcher[F],
  methodRegistry: MethodRegistry,
  headerMapping: HeaderMapping[HttpHeaders],
  codecRegistry: MessageCodecRegistry[F],
  connectHandler: ConnectHandler[F],
) extends ChannelInboundHandlerAdapter {
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

        val aGetMethod = req.method() == HttpMethod.GET

        val decodedUri = QueryStringDecoder(req.uri())
        val pathParts  = decodedUri.rawPath().substring(1).split('/').toList

        val grpcMethod = pathParts match {
          case serviceName :: methodName :: Nil =>
            // Temporary support GET-requests for all methods,
            // until https://github.com/scalapb/ScalaPB/pull/1774 is merged
            methodRegistry.get(serviceName, methodName) // .filter(_.descriptor.isSafe || aGetMethod)
          case _ =>
            None
        }

        val mediaType =
          if aGetMethod then
            decodedUri.queryParam("encoding")
              .map(MediaTypes.unsafeParseShort)
              .getOrElse(MediaTypes.`application/json`)
          else
            Option(req.headers().get(HttpHeaderNames.CONTENT_TYPE))
              .map(MediaTypes.unsafeParse)
              .getOrElse(MediaTypes.`application/json`)

        given MessageCodec[F] = codecRegistry.byMediaType(mediaType).get

        val responseF = grpcMethod match {
          case Some(methodEntry) =>
            val message: String | Stream[F, Byte] =
              if aGetMethod then decodedUri.queryParam("message").getOrElse("")
              else Stream.chunk(Chunk.array(ByteBufUtil.getBytes(req.content())))

            val entity = RequestEntity[F](message, headerMapping.toMetadata(req.headers()))

            connectHandler.handle(entity, methodEntry)
          case None =>
            errorResponse("Method not found", HttpResponseStatus.BAD_REQUEST).pure[F]
        }

        dispatcher.unsafeRunAndForget {
          responseF.attempt
            .flatMap { ei =>
              val response = ei match {
                case Right(response) => response
                case Left(e) =>
                  logger.error("Error processing request", e)
                  errorResponse(e.getMessage)
              }

              async[F] {
                if (ctx.channel().isOpen) {
                  ctx.writeAndFlush(response)
                } else {
                  logger.warn("Channel is closed, cannot send response for request {}", req.uri())

                  new DefaultChannelPromise(ctx.channel()).setSuccess()
                }
              } *>
                Async[F].delay(ctx.fireChannelReadComplete())
            }
        }
    }

  private def errorResponse(
    msg: String,
    httpCode: HttpResponseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR,
  ): FullHttpResponse = {
    val response = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      httpCode,
      Unpooled.wrappedBuffer(msg.getBytes),
    )
    response.headers()
      .set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8")
      .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes())

    response
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    logger.error("Fatal exception caught", cause)
    ctx.close()
  }

}

object HttpServerHandler {

  private val UnitRight: Either[Throwable, Unit] = Right(())

  extension (inline uri: QueryStringDecoder) {
    inline def queryParam(inline name: String): Option[String] =
      Option(uri.parameters.get(name))
        .flatMap { list =>
          if !list.isEmpty then Some(list.get(0)) else None
        }
  }

  private def async[F[_]: Async](cf: => ChannelFuture): F[Unit] =
    Async[F].async { cb =>
      Async[F].delay {
        cf.addListener { (f: ChannelFuture) =>
          if f.isSuccess then cb(UnitRight)
          else cb(Left(f.cause()))
        }

        None
      }
    }

}
