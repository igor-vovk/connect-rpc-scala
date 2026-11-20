package me.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import fs2.{Pipe, Stream}
import fs2.compression.Compression
import org.http4s.ContentCoding
import org.http4s.headers.{`Content-Encoding`, `Content-Length`}
import me.ivovk.connect_rpc_scala.ConnectException

class Compressor[F[_]: Sync] {

  given Compression[F] = Compression.forSync[F]

  def decompress(encoding: ContentCoding): Pipe[F, Byte, Byte] = in =>
    encoding match {
      case ContentCoding.gzip =>
        in.through(Compression[F].gunzip().andThen(_.flatMap(_.content)))
      case ContentCoding.identity =>
        in
      case other =>
        Stream.raiseError[F](unsupportedCompression(other))
    }

  def compress(encoding: ContentCoding)(entity: EncodedEntity[F]): F[EncodedEntity[F]] =
    encoding match {
      case ContentCoding.gzip =>
        Sync[F].pure(
          EncodedEntity(
            headers = entity.headers
              .transform(_.filterNot(_.name == `Content-Length`.name))
              .put(`Content-Encoding`(ContentCoding.gzip)),
            body = entity.body.through(Compression[F].gzip()),
          )
        )
      case ContentCoding.identity =>
        Sync[F].pure(entity)
      case other =>
        Sync[F].raiseError(unsupportedCompression(other))
    }

  private def unsupportedCompression(encoding: ContentCoding): ConnectException =
    ConnectException(
      connectrpc.Code.Unimplemented,
      s"Unsupported compression ${encoding.coding}. Supported encodings are identity and gzip.",
    )

}
