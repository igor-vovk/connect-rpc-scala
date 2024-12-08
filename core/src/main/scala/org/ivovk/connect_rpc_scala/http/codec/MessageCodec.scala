package org.ivovk.connect_rpc_scala.http.codec

import org.http4s.headers.{`Content-Encoding`, `Content-Type`}
import org.http4s.{ContentCoding, DecodeResult, Entity, EntityEncoder, Header, Headers, MediaType}
import org.ivovk.connect_rpc_scala.http.RequestEntity
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import scala.util.chaining.*

case class EncodeOptions(
  encoding: Option[ContentCoding]
)

object MessageCodec {
  given [F[_], A <: Message](using codec: MessageCodec[F], options: EncodeOptions): EntityEncoder[F, A] = {
    val headers = Headers(`Content-Type`(codec.mediaType))
      .pipe(
        options.encoding match
          case Some(encoding) => _.put(`Content-Encoding`(encoding))
          case None => identity
      )

    EntityEncoder.encodeBy(headers)(codec.encode(_, options))
  }
}

trait MessageCodec[F[_]] {

  val mediaType: MediaType

  def decode[A <: Message](m: RequestEntity[F])(using cmp: Companion[A]): DecodeResult[F, A]

  def encode[A <: Message](message: A, options: EncodeOptions): Entity[F]

}
