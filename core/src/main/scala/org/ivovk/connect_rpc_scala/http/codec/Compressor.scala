package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import fs2.Stream
import fs2.compression.Compression
import io.grpc.{Status, StatusException}
import org.http4s.ContentCoding
import org.ivovk.connect_rpc_scala.http.ResponseEntity

object Compressor {
  val supportedEncodings: Set[ContentCoding] = Set(ContentCoding.gzip)
}

class Compressor[F[_]: Sync] {

  given Compression[F] = Compression.forSync[F]

  def decompressed(encoding: Option[ContentCoding], body: Stream[F, Byte]): Stream[F, Byte] =
    encoding match {
      case Some(ContentCoding.gzip) =>
        body.through(Compression[F].gunzip().andThen(_.flatMap(_.content)))
      case Some(other) =>
        throw new StatusException(Status.INVALID_ARGUMENT.withDescription(s"Unsupported encoding: $other"))
      case None =>
        body
    }

  def compressed(encoding: Option[ContentCoding], entity: ResponseEntity[F]): ResponseEntity[F] =
    encoding match {
      case Some(ContentCoding.gzip) =>
        val coding = ContentCoding.gzip

        ResponseEntity(
          headers = entity.headers.updated("Content-Encoding", coding.coding.toLowerCase + coding.qValue),
          body = entity.body.through(Compression[F].gzip()),
        )
      case Some(other) =>
        throw new StatusException(Status.INVALID_ARGUMENT.withDescription(s"Unsupported encoding: $other"))
      case None =>
        entity
    }

}
