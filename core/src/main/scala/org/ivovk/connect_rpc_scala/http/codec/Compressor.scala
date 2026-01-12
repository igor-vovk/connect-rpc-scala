package org.ivovk.connect_rpc_scala.http.codec

import cats.Endo
import cats.effect.Sync
import fs2.Pipe
import fs2.compression.Compression
import io.grpc.Status
import org.http4s.ContentCoding
import org.http4s.util.StringWriter

object Compressor {
  val supportedEncodings: Set[ContentCoding] = Set(ContentCoding.gzip, ContentCoding.identity)
}

class Compressor[F[_]: Sync] {

  given Compression[F] = Compression.forSync[F]

  def decompress(encoding: ContentCoding): Pipe[F, Byte, Byte] = in =>
    encoding match {
      case ContentCoding.gzip =>
        in.through(Compression[F].gunzip().andThen(_.flatMap(_.content)))
      case ContentCoding.identity =>
        in
      case other =>
        throw Status.INVALID_ARGUMENT.withDescription(s"Unsupported encoding: $other").asException()
    }

  def compress(encoding: ContentCoding): Endo[EncodedEntity[F]] = entity =>
    encoding match {
      case ContentCoding.gzip =>
        val coding = ContentCoding.gzip
        val writer = new StringWriter()
        coding.render(writer)

        EncodedEntity(
          headers = entity.headers
            .updated("Content-Encoding", writer.result)
            .removed("Content-Length"), // Length is unknown after compression
          body = entity.body.through(Compression[F].gzip()),
        )
      case ContentCoding.identity =>
        entity
      case other =>
        throw Status.INVALID_ARGUMENT.withDescription(s"Unsupported encoding: $other").asException()
    }

}
