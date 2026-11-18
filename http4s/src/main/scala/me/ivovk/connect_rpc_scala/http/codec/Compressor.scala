package me.ivovk.connect_rpc_scala.http.codec

import cats.Endo
import cats.effect.Sync
import fs2.Pipe
import fs2.compression.Compression
import org.http4s.ContentCoding
import org.http4s.headers.{`Content-Encoding`, `Content-Length`}
import me.ivovk.connect_rpc_scala.service.ConnectException

class Compressor[F[_]: Sync] {

  given Compression[F] = Compression.forSync[F]

  def decompress(encoding: ContentCoding): Pipe[F, Byte, Byte] = in =>
    encoding match {
      case ContentCoding.gzip =>
        in.through(Compression[F].gunzip().andThen(_.flatMap(_.content)))
      case ContentCoding.identity =>
        in
      case other =>
        throw ConnectException(connectrpc.Code.InvalidArgument, s"Unsupported encoding: $other")
    }

  def compress(encoding: ContentCoding): Endo[EncodedEntity[F]] = entity =>
    encoding match {
      case ContentCoding.gzip =>
        EncodedEntity(
          headers = entity.headers
            .transform(_.filterNot(_.name == `Content-Length`.name))
            .put(`Content-Encoding`(ContentCoding.gzip)),
          body = entity.body.through(Compression[F].gzip()),
        )
      case ContentCoding.identity =>
        entity
      case other =>
        throw ConnectException(connectrpc.Code.InvalidArgument, s"Unsupported encoding: $other")
    }

}
