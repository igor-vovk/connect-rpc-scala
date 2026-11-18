package me.ivovk.connect_rpc_scala.http.codec

import cats.effect.{Async, Sync}
import cats.implicits.*
import fs2.{Chunk, Stream}
import org.http4s.headers.`Content-Type`
import org.http4s.{Headers, InvalidMessageBodyFailure, MediaType}
import me.ivovk.connect_rpc_scala.http.MediaTypes
import me.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.slf4j.LoggerFactory
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.nio.charset.Charset
import java.util.Base64

class ProtoMessageCodec[F[_]: Async] extends MessageCodec[F] {

  private val logger     = LoggerFactory.getLogger(getClass)
  private val base64dec  = Base64.getUrlDecoder
  private val compressor = Compressor[F]()

  override val mediaType: MediaType = MediaTypes.`application/proto`

  private val headersToEncode = Headers(`Content-Type`(mediaType))

  override def decode[A <: Message](entity: EntityToDecode[F])(using cmp: Companion[A]): Stream[F, A] = {
    val messages = entity.message match {
      case str: String =>
        Stream.eval(Async[F].delay(cmp.parseFrom(base64dec.decode(str.getBytes(entity.charset)))))
      case stream: Stream[F, Byte] =>
        stream.through(compressor.decompress(entity.encoding))
          .chunkAll
          .evalMap { chunk =>
            Async[F].delay(cmp.parseFrom(chunk.toArray))
          }
    }

    messages
      .pipeIf(logger.isTraceEnabled) {
        _.map { msg =>
          logger.trace(s">>> Proto: ${msg.toProtoString}")
          msg
        }
      }
      .adaptError(e => InvalidMessageBodyFailure(e.getMessage, e.some))
  }

  private[codec] override def encodeChunk(message: Message, _charset: Charset): Chunk[Byte] = {
    if (logger.isTraceEnabled) {
      logger.trace(s"<<< Proto: ${message.toProtoString}")
    }

    Chunk.array(message.toByteArray)
  }

  override def encode[A <: Message](message: A, options: EncodeOptions)(
    using F: Sync[F]
  ): F[EncodedEntity[F]] =
    F.delay {
      EncodedEntity.fromChunk[F](headersToEncode, encodeChunk(message, options.charset))
        .pipe(compressor.compress(options.encoding))
    }

}
