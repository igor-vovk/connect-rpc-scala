package org.ivovk.connect_rpc_scala.http

import cats.Applicative
import cats.data.EitherT
import cats.effect.{Async, Sync}
import cats.implicits.*
import com.google.protobuf.CodedOutputStream
import fs2.compression.Compression
import fs2.io.{readOutputStream, toInputStreamResource}
import fs2.text.decodeWithCharset
import fs2.{Chunk, Stream}
import org.http4s.headers.`Content-Type`
import org.http4s.{ContentCoding, DecodeResult, Entity, EntityDecoder, EntityEncoder, MediaRange, MediaType}
import org.ivovk.connect_rpc_scala.ConnectRpcHttpRoutes.getClass
import org.slf4j.{Logger, LoggerFactory}
import scalapb.json4s.{JsonFormat, Printer}
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.net.URLDecoder
import java.util.Base64

object MessageCodec {
  given [F[_] : Applicative, A <: Message](using codec: MessageCodec[F], cmp: Companion[A]): EntityDecoder[F, A] =
    EntityDecoder.decodeBy(MediaRange.`*/*`)(m => codec.decode(RequestEntity(m)))

  given [F[_], A <: Message](using codec: MessageCodec[F]): EntityEncoder[F, A] =
    EntityEncoder.encodeBy(`Content-Type`(codec.mediaType))(codec.encode)
}

trait MessageCodec[F[_]] {

  val mediaType: MediaType

  def decode[A <: Message](m: RequestEntity[F])(using cmp: Companion[A]): DecodeResult[F, A]

  def encode[A <: Message](message: A): Entity[F]

}

class JsonMessageCodec[F[_] : Sync : Compression](printer: Printer) extends MessageCodec[F] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override val mediaType: MediaType = MediaTypes.`application/json`

  override def decode[A <: Message](entity: RequestEntity[F])(using cmp: Companion[A]): DecodeResult[F, A] = {
    val charset = entity.charset.nioCharset
    val string  = entity.message match {
      case str: String =>
        Sync[F].delay(URLDecoder.decode(str, charset))
      case stream: Stream[F, Byte] =>
        decompressed(entity.encoding, stream)
          .through(decodeWithCharset(charset))
          .compile.string
    }

    val f = string
      .flatMap { str =>
        if (logger.isTraceEnabled) {
          logger.trace(s">>> Headers: ${entity.headers.redactSensitive}")
          logger.trace(s">>> JSON: $str")
        }

        Sync[F].delay(JsonFormat.fromJsonString(str))
      }

    EitherT.right(f)
  }

  override def encode[A <: Message](message: A): Entity[F] = {
    val string = printer.print(message)

    if (logger.isTraceEnabled) {
      logger.trace(s"<<< JSON: $string")
    }

    val bytes = string.getBytes()

    Entity(
      body = Stream.chunk(Chunk.array(bytes)),
      length = Some(bytes.length.toLong),
    )
  }

}

class ProtoMessageCodec[F[_] : Async : Compression] extends MessageCodec[F] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val base64dec = Base64.getUrlDecoder

  override val mediaType: MediaType = MediaTypes.`application/proto`

  override def decode[A <: Message](entity: RequestEntity[F])(using cmp: Companion[A]): DecodeResult[F, A] = {
    val f = entity.message match {
      case str: String =>
        Async[F].delay(base64dec.decode(str.getBytes(entity.charset.nioCharset)))
          .flatMap(arr => Async[F].delay(cmp.parseFrom(arr)))
      case stream: Stream[F, Byte] =>
        toInputStreamResource(decompressed(entity.encoding, stream))
          .use(is => Async[F].delay(cmp.parseFrom(is)))
    }

    EitherT.right(f.map { message =>
      if (logger.isTraceEnabled) {
        logger.trace(s">>> Headers: ${entity.headers.redactSensitive}")
        logger.trace(s">>> Proto: ${message.toProtoString}")
      }

      message
    })
  }

  override def encode[A <: Message](message: A): Entity[F] = {
    if (logger.isTraceEnabled) {
      logger.trace(s"<<< Proto: ${message.toProtoString}")
    }

    val dataLength = message.serializedSize
    val chunkSize  = CodedOutputStream.DEFAULT_BUFFER_SIZE min dataLength

    Entity(
      body = readOutputStream(chunkSize)(os => Async[F].delay(message.writeTo(os))),
      length = Some(dataLength.toLong),
    )
  }

}

def decompressed[F[_] : Compression](encoding: Option[ContentCoding], body: Stream[F, Byte]): Stream[F, Byte] = {
  body.through(encoding match {
    case Some(ContentCoding.gzip) =>
      Compression[F].gunzip().andThen(_.flatMap(_.content))
    case _ =>
      identity
  })
}