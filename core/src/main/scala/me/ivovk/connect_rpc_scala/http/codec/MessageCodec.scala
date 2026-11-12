package me.ivovk.connect_rpc_scala.http.codec

import fs2.Stream
import org.http4s.{ContentCoding, MediaType}
import me.ivovk.connect_rpc_scala.service.ConnectHeaders
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
  headers: ConnectHeaders,
) {
  @threadUnsafe
  private lazy val contentType: Option[String] =
    headers.get("content-type").flatMap(_.headOption)

  def charset: Charset =
    contentType
      .flatMap(_.split(';').iterator.map(_.trim).find(_.startsWith("charset=")))
      .map(_.stripPrefix("charset="))
      .map(Charset.forName)
      .getOrElse(StandardCharsets.UTF_8)

  @threadUnsafe
  lazy val encoding: ContentCoding =
    headers.get("content-encoding").flatMap(_.headOption)
      .map(ContentCoding.unsafeFromString)
      .getOrElse(ContentCoding.identity)

  def as[A <: Message: Companion](using codec: MessageCodec[F]): Stream[F, A] =
    codec.decode(this)(using summon[Companion[A]])
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
