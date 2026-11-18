package me.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import fs2.{Chunk, Stream}
import org.http4s.headers.{`Content-Encoding`, `Content-Length`, `Content-Type`, `Transfer-Encoding`}
import org.http4s.{ContentCoding, Headers, MediaType}
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
  lazy val charset: Charset =
    headers.get[`Content-Type`].flatMap(_.charset)
      .fold(StandardCharsets.UTF_8)(_.nioCharset)

  @threadUnsafe
  lazy val encoding: ContentCoding =
    headers.get[`Content-Encoding`]
      .fold(ContentCoding.identity)(_.contentCoding)

  def as[A <: Message: Companion](using codec: MessageCodec[F]): Stream[F, A] =
    codec.decode(this)(using summon[Companion[A]])

  def decodeOne[A <: Message](
    companion: Companion[A],
    codec: MessageCodec[F],
  )(using Sync[F]): F[A] =
    Sync[F].defer(as[A](using companion, codec).compile.onlyOrError)
}

case class EncodedEntity[F[_]](
  headers: Headers,
  body: Stream[F, Byte],
) {
  private[connect_rpc_scala] def headersWithMetadata(metadata: Headers): Headers =
    metadata.transform(_.filterNot { header =>
      header.name == `Content-Length`.name || header.name == `Transfer-Encoding`.name
    }) ++ headers
}

object EncodedEntity {
  private[codec] def fromChunk[F[_]](headers: Headers, chunk: Chunk[Byte]): EncodedEntity[F] =
    EncodedEntity(headers.put(`Content-Length`.unsafeFromLong(chunk.size.toLong)), Stream.chunk(chunk))
}

trait MessageCodec[F[_]] {

  val mediaType: MediaType

  def decode[A <: Message](m: EntityToDecode[F])(using cmp: Companion[A]): Stream[F, A]

  def encode[A <: Message](message: Stream[F, A], options: EncodeOptions): EncodedEntity[F]

  private[connect_rpc_scala] def encodeUnary[A <: Message](
    message: A,
    options: EncodeOptions,
  )(using F: Sync[F]): F[EncodedEntity[F]] =
    F.delay(encode(Stream.emit(message), options))

}
