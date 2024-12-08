package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import fs2.Stream
import fs2.compression.Compression
import org.http4s.{ContentCoding, Entity}

object Compressor {
  val supportedEncodings: Set[ContentCoding] = Set(ContentCoding.gzip)
}

class Compressor[F[_] : Sync] {

  given Compression[F] = Compression.forSync[F]

  def decompressed(encoding: Option[ContentCoding], body: Stream[F, Byte]): Stream[F, Byte] =
    body.through(encoding match {
      case Some(ContentCoding.gzip) =>
        Compression[F].gunzip().andThen(_.flatMap(_.content))
      case Some(other) =>
        throw new IllegalArgumentException(s"Unsupported encoding: $other")
      case None =>
        identity
    })

  def compressed(encoding: Option[ContentCoding], entity: Entity[F]): Entity[F] =
    encoding match {
      case Some(ContentCoding.gzip) =>
        Entity(
          body = entity.body.through(Compression[F].gzip()),
        )
      case Some(other) =>
        throw new IllegalArgumentException(s"Unsupported encoding: $other")
      case None =>
        entity
    }

}
