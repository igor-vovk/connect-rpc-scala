package org.ivovk.connect_rpc_scala.http

import org.http4s.MediaType

case class MCEntry[F[_]](
  encoder: MessageEncoder[F],
  decoder: MessageDecoder[F],
)


object MessageCoderRegistry {

  def apply[F[_]](encoders: MCEntry[F]*): MessageCoderRegistry[F] =
    new MessageCoderRegistry[F](encoders.map(e => e.encoder.mediaType -> e).toMap)

}

class MessageCoderRegistry[F[_]](encoders: Map[MediaType, MCEntry[F]]) {

  def fromContentType(mediaType: MediaType): Option[MCEntry[F]] = encoders.get(mediaType)

}
