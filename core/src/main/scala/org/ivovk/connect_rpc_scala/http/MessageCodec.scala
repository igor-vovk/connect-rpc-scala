package org.ivovk.connect_rpc_scala.http

import cats.Applicative
import cats.data.EitherT
import cats.effect.{Async, Sync}
import cats.implicits.*
import fs2.compression.Compression
import fs2.io.{readOutputStream, toInputStreamResource}
import fs2.text.decodeWithCharset
import org.http4s.headers.{`Content-Encoding`, `Content-Type`}
import org.http4s.{Charset, ContentCoding, DecodeResult, Entity, EntityDecoder, EntityEncoder, Media, MediaRange, MediaType}
import org.ivovk.connect_rpc_scala.ConnectRpcHttpRoutes.getClass
import org.slf4j.{Logger, LoggerFactory}
import scalapb.json4s.{JsonFormat, Printer}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

trait MessageCodec[F[_]] {

  val mediaType: MediaType

  def decode[A <: GeneratedMessage](m: Media[F])(using cmp: GeneratedMessageCompanion[A]): DecodeResult[F, A]

  def encode[A <: GeneratedMessage](message: A): Entity[F]

}

object MessageCodec {
  given [F[_] : Applicative, A <: GeneratedMessage](
    using codec: MessageCodec[F], cmp: GeneratedMessageCompanion[A]
  ): EntityDecoder[F, A] =
    EntityDecoder.decodeBy(MediaRange.`*/*`)(codec.decode)

  given [F[_], A <: GeneratedMessage](
    using codec: MessageCodec[F]
  ): EntityEncoder[F, A] =
    EntityEncoder.encodeBy(`Content-Type`(codec.mediaType))(codec.encode)
}

class JsonMessageCodec[F[_] : Sync : Compression](jsonPrinter: Printer) extends MessageCodec[F] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override val mediaType: MediaType = MediaType.application.`json`

  override def decode[A <: GeneratedMessage](m: Media[F])(using cmp: GeneratedMessageCompanion[A]): DecodeResult[F, A] = {
    val charset = m.charset.getOrElse(Charset.`UTF-8`).nioCharset

    val f = decompressed(m)
      .through(decodeWithCharset(charset))
      .compile.string
      .flatMap { str =>
        if (logger.isTraceEnabled) {
          logger.trace(s">>> Headers: ${m.headers}")
          logger.trace(s">>> JSON: $str")
        }

        Sync[F].delay(JsonFormat.fromJsonString(str))
      }

    EitherT.right(f)
  }

  override def encode[A <: GeneratedMessage](message: A): Entity[F] = {
    val string = jsonPrinter.print(message)

    if (logger.isTraceEnabled) {
      logger.trace(s"<<< JSON: $string")
    }

    EntityEncoder.stringEncoder[F].toEntity(string)
  }

}

class ProtoMessageCodec[F[_] : Async : Compression] extends MessageCodec[F] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override val mediaType: MediaType =
    MediaType.unsafeParse("application/proto")

  override def decode[A <: GeneratedMessage](m: Media[F])(using cmp: GeneratedMessageCompanion[A]): DecodeResult[F, A] = {
    val f = toInputStreamResource(decompressed(m)).use { is =>
      Async[F].delay {
        val message = cmp.parseFrom(is)

        if (logger.isTraceEnabled) {
          logger.trace(s">>> Headers: ${m.headers}")
          logger.trace(s">>> Proto: ${message.toProtoString}")
        }

        message
      }
    }

    EitherT.right(f)
  }

  override def encode[A <: GeneratedMessage](message: A): Entity[F] = {
    if (logger.isTraceEnabled) {
      logger.trace(s"<<< Proto: ${message.toProtoString}")
    }

    Entity(
      body = readOutputStream(2048)(os => Async[F].delay(message.writeTo(os))),
      length = Some(message.serializedSize.toLong),
    )
  }

}

def decompressed[F[_] : Compression](m: Media[F]): fs2.Stream[F, Byte] = {
  val encoding = m.headers.get[`Content-Encoding`].map(_.contentCoding)

  m.body.through(encoding match {
    case Some(ContentCoding.gzip) =>
      Compression[F].gunzip().andThen(_.flatMap(_.content))
    case _ =>
      identity
  })
}