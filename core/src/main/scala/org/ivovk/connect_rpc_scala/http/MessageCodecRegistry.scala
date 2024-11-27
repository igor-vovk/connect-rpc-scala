package org.ivovk.connect_rpc_scala.http

import org.http4s.MediaType

object MessageCodecRegistry {

  def apply[F[_]](encoders: MessageCodec[F]*): MessageCodecRegistry[F] =
    new MessageCodecRegistry[F](encoders.map(e => e.mediaType -> e).toMap)

}

class MessageCodecRegistry[F[_]] private(encoders: Map[MediaType, MessageCodec[F]]) {

  def byContentType(mediaType: MediaType): Option[MessageCodec[F]] = encoders.get(mediaType)

}