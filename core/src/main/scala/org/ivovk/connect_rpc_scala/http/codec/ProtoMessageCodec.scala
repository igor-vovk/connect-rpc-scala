package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Async
import cats.implicits.*
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream
import fs2.{Chunk, Stream}
import org.http4s.{InvalidMessageBodyFailure, MediaType}
import org.ivovk.connect_rpc_scala.http.MediaTypes
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.slf4j.LoggerFactory
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.util.Base64

class ProtoMessageCodec[F[_]: Async] extends MessageCodec[F] {

  private val logger     = LoggerFactory.getLogger(getClass)
  private val base64dec  = Base64.getUrlDecoder
  private val compressor = Compressor[F]()

  override val mediaType: MediaType = MediaTypes.`application/proto`

  override def decode[A <: Message](entity: EntityToDecode[F])(using cmp: Companion[A]): Stream[F, A] = {
    val msg = entity.message match {
      case str: String =>
        Async[F].delay(cmp.parseFrom(base64dec.decode(str.getBytes(entity.charset))))
      case stream: Stream[F, Byte] =>
        stream.through(compressor.decompress(entity.encoding))
          .chunkAll
          .evalMap { chunk =>
            Async[F].delay(cmp.parseFrom(ByteBufferBackedInputStream(chunk.toByteBuffer)))
          }
          .compile.onlyOrError
    }

    Stream.eval(msg)
      .pipeIf(logger.isTraceEnabled) {
        _.map { msg =>
          logger.trace(s">>> Proto: ${msg.toProtoString}")
          msg
        }
      }
      .adaptError(e => InvalidMessageBodyFailure(e.getMessage, e.some))
  }

  override def encode[A <: Message](message: Stream[F, A], options: EncodeOptions): EncodedEntity[F] = {
    val body = message.flatMap { msg =>
      if (logger.isTraceEnabled) {
        logger.trace(s"<<< Proto: ${msg.toProtoString}")
      }

      Stream.chunk(Chunk.array(msg.toByteArray))
    }

    val entity = EncodedEntity[F](
      headers = Map(
        "Content-Type" -> mediaType.show
      ),
      body = body,
    )

    entity.pipe(compressor.compress(options.encoding))
  }

}
