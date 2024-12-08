package org.ivovk.connect_rpc_scala.http.codec

import org.http4s.headers.`Content-Type`
import org.http4s.{DecodeResult, Entity, EntityEncoder, MediaType}
import org.ivovk.connect_rpc_scala.http.RequestEntity
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

object MessageCodec {
  given [F[_], A <: Message](using codec: MessageCodec[F]): EntityEncoder[F, A] =
    EntityEncoder.encodeBy(`Content-Type`(codec.mediaType))(codec.encode)
}

trait MessageCodec[F[_]] {

  val mediaType: MediaType

  def decode[A <: Message](m: RequestEntity[F])(using cmp: Companion[A]): DecodeResult[F, A]

  def encode[A <: Message](message: A): Entity[F]

}
