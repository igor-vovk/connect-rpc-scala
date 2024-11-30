package org.ivovk.connect_rpc_scala.http

import cats.MonadThrow
import fs2.Stream
import org.http4s.{Charset, Headers, Media}
import org.http4s.headers.`Content-Type`
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

object RequestEntity {
  def apply[F[_]](m: Media[F]): RequestEntity[F] =
    RequestEntity(m.body, m.headers)
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
  def contentType: Option[`Content-Type`] =
    headers.get[`Content-Type`]

  def charset: Charset =
    contentType.flatMap(_.charset).getOrElse(Charset.`UTF-8`)

  def as[A <: Message](using M: MonadThrow[F], codec: MessageCodec[F], cmp: Companion[A]): F[A] =
    M.rethrow(codec.decode(this).value)

  def fold[A](string: String => A)(stream: Stream[F, Byte] => A): A =
    message match {
      case s: String =>
        string(s)
      case b: Stream[F, Byte] =>
        stream(b)
    }
}
