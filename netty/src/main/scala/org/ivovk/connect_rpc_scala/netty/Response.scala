package org.ivovk.connect_rpc_scala.netty

import cats.effect.Async
import cats.implicits.*
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.*
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, MessageCodec}
import org.slf4j.LoggerFactory
import scalapb.GeneratedMessage

object Response {

  private val logger = LoggerFactory.getLogger(getClass)

  def create[F[_]: Async](
    message: GeneratedMessage,
    status: HttpResponseStatus = HttpResponseStatus.OK,
    headers: HttpHeaders = EmptyHttpHeaders.INSTANCE,
  )(using codec: MessageCodec[F], options: EncodeOptions): F[HttpResponse] = {
    val responseEntity = codec.encode(message, options)

    responseEntity.body.chunks.map(ByteBufConversions.chunkToByteBuf).compile.toList
      .map { byteBufs =>
        val response = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1,
          status,
          Unpooled.wrappedBuffer(byteBufs*),
        )

        response.headers().add(headers)
        responseEntity.headers.foreach((name, value) => response.headers().set(name, value))
        responseEntity.length.foreach(response.headers().set(HttpHeaderNames.CONTENT_LENGTH, _))

        if (logger.isTraceEnabled) {
          logger.trace(s"<<< Headers: ${response.headers()}")
        }

        response
      }
  }

}
