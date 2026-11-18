package me.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import fs2.Stream
import org.http4s.headers.`Content-Type`
import org.http4s.{ContentCoding, Headers, MediaType}
import org.typelevel.ci.*
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.nio.charset.{Charset, StandardCharsets}
import scala.annotation.threadUnsafe

case class EncodeOptions(
  charset: Charset,
  encoding: ContentCoding,
)

/**
 * Encoded message and headers with the knowledge how this message can be decoded.
 *
 * Similar to [[org.http4s.Media]], but extends the message with `String` type representing message that is
 * passed in a query parameter.
 */
case class EntityToDecode[F[_]](
  message: String | Stream[F, Byte],
  headers: Headers,
) {
  @threadUnsafe
  private lazy val contentType: Option[`Content-Type`] =
    headers.get(ci"Content-Type")
      .map(header => `Content-Type`.parse(header.head.value).fold(throw _, identity))

  def charset: Charset =
    contentType.flatMap(_.charset).fold(StandardCharsets.UTF_8)(_.nioCharset)

  @threadUnsafe
  lazy val encoding: ContentCoding =
    headers.get(ci"Content-Encoding")
      .map(header => ContentCoding.unsafeFromString(header.head.value))
      .getOrElse(ContentCoding.identity)

  def as[A <: Message: Companion](using codec: MessageCodec[F]): Stream[F, A] =
    codec.decode(this)(using summon[Companion[A]])

  def decodeOne[A <: Message](
    companion: Companion[A],
    codec: MessageCodec[F],
  )(using Sync[F]): F[A] =
    Sync[F].defer(as[A](using companion, codec).compile.onlyOrError)
}

case class EncodedEntity[F[_]](
  headers: Map[String, String],
  body: Stream[F, Byte],
)

trait MessageCodec[F[_]] {

  val mediaType: MediaType

  def decode[A <: Message](m: EntityToDecode[F])(using cmp: Companion[A]): Stream[F, A]

  def encode[A <: Message](message: Stream[F, A], options: EncodeOptions): EncodedEntity[F]

}
