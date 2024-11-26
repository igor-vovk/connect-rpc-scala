package org.ivovk.connect_rpc_scala.http

import cats.data.EitherT
import cats.effect.{Async, Sync}
import cats.implicits.*
import fs2.compression.Compression
import fs2.io.toInputStreamResource
import fs2.text.decodeWithCharset
import org.http4s.headers.`Content-Encoding`
import org.http4s.{Charset, ContentCoding, DecodeResult, EntityDecoder, Media, MediaRange}
import org.ivovk.connect_rpc_scala.ConnectRpcHttpRoutes.getClass
import org.slf4j.{Logger, LoggerFactory}
import scalapb.json4s.JsonFormat
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

trait MessageDecoder[F[_]] {

  def decode[A <: GeneratedMessage](m: Media[F])(using cmp: GeneratedMessageCompanion[A]): DecodeResult[F, A]

}

class JsonMessageDecoder[F[_] : Sync : Compression] extends MessageDecoder[F] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

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

}

class ProtoMessageDecoder[F[_] : Async : Compression] extends MessageDecoder[F] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

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