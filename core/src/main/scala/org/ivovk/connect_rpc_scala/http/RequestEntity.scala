package org.ivovk.connect_rpc_scala.http

import cats.MonadThrow
import fs2.Stream
import org.http4s.headers.{`Content-Encoding`, `Content-Type`}
import org.http4s.{Charset, ContentCoding, Headers}
import org.ivovk.connect_rpc_scala.http.Headers.`Connect-Timeout-Ms`
import org.ivovk.connect_rpc_scala.http.codec.MessageCodec
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}


object RequestEntity {
  extension (h: Headers) {
    def timeout: Option[Long] =
      h.get[`Connect-Timeout-Ms`].map(_.value)
  }
}

/**
 * Encoded message and headers with the knowledge how this message can be decoded.
 * Similar to [[org.http4s.Media]], but extends the message with `String` type representing message that is
 * passed in a query parameter.
 */
case class RequestEntity[F[_]](
  message: String | Stream[F, Byte],
  headers: Headers,
) {
  import RequestEntity.*

  private def contentType: Option[`Content-Type`] =
    headers.get[`Content-Type`]

  def charset: Charset =
    contentType.flatMap(_.charset).getOrElse(Charset.`UTF-8`)

  def encoding: Option[ContentCoding] =
    headers.get[`Content-Encoding`].map(_.contentCoding)

  def timeout: Option[Long] = headers.timeout

  def as[A <: Message: Companion](using M: MonadThrow[F], codec: MessageCodec[F]): F[A] =
    M.rethrow(codec.decode(this)(using summon[Companion[A]]).value)
}
