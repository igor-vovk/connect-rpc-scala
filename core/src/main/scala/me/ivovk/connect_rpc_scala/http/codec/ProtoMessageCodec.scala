package me.ivovk.connect_rpc_scala.http.codec

import cats.effect.{Async, Sync}
import cats.implicits.*
import fs2.{Chunk, Stream}
import org.http4s.headers.`Content-Type`
import org.http4s.{Headers, InvalidMessageBodyFailure, MediaType}
import me.ivovk.connect_rpc_scala.ConnectException
import me.ivovk.connect_rpc_scala.http.MediaTypes
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

  override def decode[A <: Message](
    message: Stream[F, Byte] | String,
    options: DecodeOptions,
  )(using cmp: Companion[A]): F[A] = {
    def parse(bytes: Array[Byte]): A = {
      val parsed = cmp.parseFrom(bytes)
      if (logger.isTraceEnabled) {
        logger.trace(s">>> Proto: ${parsed.toProtoString}")
      }
      parsed
    }

    val decoded = message match {
      case str: String =>
        Async[F].delay(parse(base64dec.decode(str.getBytes(options.charset))))
      case stream: Stream[F, Byte] =>
        stream.through(compressor.decompress(options.encoding))
          .compile.to(Chunk)
          .flatMap(chunk => Async[F].delay(parse(chunk.toArray)))
    }

    decoded.adaptError {
      case e: ConnectException => e
      case error               =>
        InvalidMessageBodyFailure(error.getMessage, error.some)
    }
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
    F.delay(EncodedEntity.fromChunk[F](headersToEncode, encodeChunk(message, options.charset)))
      .flatMap(compressor.compress(options.encoding))

}
