package org.ivovk.connect_rpc_scala.http.codec

import org.http4s.MediaType

object MessageCodecRegistry {

  def apply[F[_]](codecs: MessageCodec[F]*): MessageCodecRegistry[F] =
    new MessageCodecRegistry[F](codecs.map(e => e.mediaType -> e).toMap)

}

class MessageCodecRegistry[F[_]] private (codecs: Map[MediaType, MessageCodec[F]]) {

  def byMediaType(mediaType: MediaType): Option[MessageCodec[F]] = codecs.get(mediaType)

}
