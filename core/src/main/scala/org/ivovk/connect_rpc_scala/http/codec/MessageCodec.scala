package org.ivovk.connect_rpc_scala.http.codec

import fs2.Stream
import io.grpc.Metadata
import org.http4s.{ContentCoding, MediaType}
import org.ivovk.connect_rpc_scala.grpc.GrpcHeaders
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.nio.charset.Charset
import scala.annotation.threadUnsafe

case class EncodeOptions(
  encoding: ContentCoding
)

object EncodeOptions {
  given Default: EncodeOptions = EncodeOptions(ContentCoding.identity)
}

/**
 * Encoded message and headers with the knowledge how this message can be decoded.
 *
 * Similar to [[org.http4s.Media]], but extends the message with `String` type representing message that is
 * passed in a query parameter.
 */
case class EntityToDecode[F[_]](
  message: String | Stream[F, Byte],
  headers: Metadata,
) {
  @threadUnsafe
  private lazy val contentType: Option[GrpcHeaders.ContentType] =
    Option(headers.get(GrpcHeaders.ContentTypeKey))

  def charset: Charset =
    contentType.flatMap(_.nioCharset).getOrElse(Charset.defaultCharset())

  private def isStreaming = contentType.exists(_.mediaType.startsWith("application/connect+"))

  @threadUnsafe
  lazy val encoding: ContentCoding = {
    val key =
      if isStreaming then GrpcHeaders.StreamingContentEncodingKey
      else GrpcHeaders.ContentEncodingKey

    Option(headers.get(key))
      .map(ContentCoding.unsafeFromString)
      .getOrElse(ContentCoding.identity)
  }

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

  def encode[A <: Message](message: A, options: EncodeOptions): EncodedEntity[F]

}
