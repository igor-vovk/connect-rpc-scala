package me.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import fs2.{Chunk, Stream}
import org.http4s.headers.{`Content-Encoding`, `Content-Length`, `Content-Type`}
import org.http4s.{ContentCoding, Headers, MediaType}
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.nio.charset.{Charset, StandardCharsets}

case class DecodeOptions(
  charset: Charset,
  encoding: ContentCoding,
)

object DecodeOptions {
  def fromHeaders(headers: Headers): DecodeOptions = {
    val charset: Charset = headers.get[`Content-Type`].flatMap(_.charset)
      .fold(StandardCharsets.UTF_8)(_.nioCharset)

    val encoding: ContentCoding = headers.get[`Content-Encoding`]
      .fold(ContentCoding.identity)(_.contentCoding)

    DecodeOptions(charset, encoding)
  }
}

case class EncodeOptions(
  charset: Charset,
  encoding: ContentCoding,
)

case class EncodedEntity[F[_]](
  headers: Headers,
  body: Stream[F, Byte],
)

object EncodedEntity {
  def fromChunk[F[_]](headers: Headers, chunk: Chunk[Byte]): EncodedEntity[F] =
    EncodedEntity(headers.put(`Content-Length`(chunk.size)), Stream.chunk(chunk))
}

trait MessageCodec[F[_]] {

  val mediaType: MediaType

  def decode[A <: Message](
    message: Stream[F, Byte] | String,
    options: DecodeOptions,
  )(using cmp: Companion[A]): F[A]

  /**
   * Encodes one message and supplies `Content-Length` when the encoded byte length is known. This lets
   * HTTP/1.1 transports skip chunked framing and its encoding and parsing work.
   */
  def encode[A <: Message](
    message: A,
    options: EncodeOptions,
  )(using F: Sync[F]): F[EncodedEntity[F]]

  private[codec] def encodeChunk(message: Message, charset: Charset): Chunk[Byte]

}
