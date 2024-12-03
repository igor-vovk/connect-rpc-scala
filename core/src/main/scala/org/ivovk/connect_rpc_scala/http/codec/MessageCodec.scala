package org.ivovk.connect_rpc_scala.http.codec

import cats.Applicative
import org.http4s.headers.`Content-Type`
import org.http4s.{DecodeResult, Entity, EntityDecoder, EntityEncoder, MediaRange, MediaType}
import org.ivovk.connect_rpc_scala.http.RequestEntity
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

object MessageCodec {
  given [F[_] : Applicative, A <: Message](using codec: MessageCodec[F], cmp: Companion[A]): EntityDecoder[F, A] =
    EntityDecoder.decodeBy(MediaRange.`*/*`)(m => codec.decode(RequestEntity(m)))

  given [F[_], A <: Message](using codec: MessageCodec[F]): EntityEncoder[F, A] =
    EntityEncoder.encodeBy(`Content-Type`(codec.mediaType))(codec.encode)
}

trait MessageCodec[F[_]] {

  val mediaType: MediaType

  def decode[A <: Message](m: RequestEntity[F])(using cmp: Companion[A]): DecodeResult[F, A]

  def encode[A <: Message](message: A): Entity[F]

}
