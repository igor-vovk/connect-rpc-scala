package org.ivovk.connect_rpc_scala.http

import cats.Applicative
import cats.effect.{Async, Sync}
import cats.implicits.*
import com.google.protobuf.CodedOutputStream
import fs2.compression.Compression
import fs2.io.{readOutputStream, toInputStreamResource}
import fs2.text.decodeWithCharset
import fs2.{Chunk, Stream}
import org.http4s.headers.`Content-Type`
import org.http4s.{ContentCoding, DecodeResult, Entity, EntityDecoder, EntityEncoder, InvalidMessageBodyFailure, MediaRange, MediaType}
import org.slf4j.{Logger, LoggerFactory}
import scalapb.json4s.{JsonFormat, Printer}
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.net.URLDecoder
import java.util.Base64
import scala.util.chaining.*

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

class JsonMessageCodec[F[_] : Sync](compressor: Compressor[F], printer: Printer) extends MessageCodec[F] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override val mediaType: MediaType = MediaTypes.`application/json`

  override def decode[A <: Message](entity: RequestEntity[F])(using cmp: Companion[A]): DecodeResult[F, A] = {
    val charset = entity.charset.nioCharset
    val string  = entity.message match {
      case str: String =>
        Sync[F].delay(URLDecoder.decode(str, charset))
      case stream: Stream[F, Byte] =>
        compressor.decompressed(entity.encoding, stream)
          .through(decodeWithCharset(charset))
          .compile.string
    }

    string
      .flatMap { str =>
        if (logger.isTraceEnabled) {
          logger.trace(s">>> Headers: ${entity.headers.redactSensitive()}")
          logger.trace(s">>> JSON: $str")
        }

        Sync[F].delay(JsonFormat.fromJsonString(str))
      }
      .attemptT
      .leftMap(e => InvalidMessageBodyFailure(e.getMessage, e.some))
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

class ProtoMessageCodec[F[_] : Async](compressor: Compressor[F]) extends MessageCodec[F] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val base64dec = Base64.getUrlDecoder

  override val mediaType: MediaType = MediaTypes.`application/proto`

  override def decode[A <: Message](entity: RequestEntity[F])(using cmp: Companion[A]): DecodeResult[F, A] = {
    val msg = entity.message match {
      case str: String =>
        Async[F].delay(cmp.parseFrom(base64dec.decode(str.getBytes(entity.charset.nioCharset))))
      case stream: Stream[F, Byte] =>
        toInputStreamResource(compressor.decompressed(entity.encoding, stream))
          .use(is => Async[F].delay(cmp.parseFrom(is)))
    }

    msg
      .pipe(
        if logger.isTraceEnabled then
          _.map { msg =>
            logger.trace(s">>> Headers: ${entity.headers.redactSensitive()}")
            logger.trace(s">>> Proto: ${msg.toProtoString}")
            msg
          }
        else identity
      )
      .attemptT
      .leftMap(e => InvalidMessageBodyFailure(e.getMessage, e.some))
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

class Compressor[F[_] : Sync] {

  given Compression[F] = Compression.forSync[F]

  def decompressed(encoding: Option[ContentCoding], body: Stream[F, Byte]): Stream[F, Byte] =
    body.through(encoding match {
      case Some(ContentCoding.gzip) =>
        Compression[F].gunzip().andThen(_.flatMap(_.content))
      case _ =>
        identity
    })

}
