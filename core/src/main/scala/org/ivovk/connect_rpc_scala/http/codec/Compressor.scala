package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import fs2.Stream
import fs2.compression.Compression
import org.http4s.ContentCoding

class Compressor[F[_] : Sync] {

  given Compression[F] = Compression.forSync[F]

  def decompressed(encoding: Option[ContentCoding], body: Stream[F, Byte]): Stream[F, Byte] =
    body.through(encoding match {
      case Some(ContentCoding.gzip) =>
        Compression[F].gunzip().andThen(_.flatMap(_.content))
      case _ =>
        identity
    })

}
