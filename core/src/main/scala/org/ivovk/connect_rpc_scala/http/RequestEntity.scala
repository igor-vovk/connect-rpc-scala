package org.ivovk.connect_rpc_scala.http

import cats.MonadThrow
import fs2.Stream
import io.grpc.Metadata
import org.http4s.ContentCoding
import org.ivovk.connect_rpc_scala.grpc.GrpcHeaders
import org.ivovk.connect_rpc_scala.http.codec.MessageCodec
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.nio.charset.Charset

/**
 * Encoded message and headers with the knowledge how this message can be decoded.
 *
 * Similar to [[org.http4s.Media]], but extends the message with `String` type representing message that is
 * passed in a query parameter.
 */
case class RequestEntity[F[_]](
  message: String | Stream[F, Byte],
  headers: Metadata,
) {
  private def contentType: Option[GrpcHeaders.ContentType] =
    Option(headers.get(GrpcHeaders.ContentTypeKey))

  def charset: Charset = contentType.flatMap(_.nioCharset).getOrElse(Charset.defaultCharset())

  def encoding: Option[ContentCoding] =
    Option(headers.get(GrpcHeaders.ContentEncodingKey)).map(ContentCoding.unsafeFromString)

  def as[A <: Message: Companion](using M: MonadThrow[F], codec: MessageCodec[F]): F[A] =
    M.rethrow(codec.decode(this)(using summon[Companion[A]]).value)
}
