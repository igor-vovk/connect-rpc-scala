package me.ivovk.connect_rpc_scala.http.codec

import cats.data.NonEmptyList
import cats.effect.Sync
import fs2.{Pipe, Stream}
import fs2.compression.{Compression, DeflateParams, InflateParams}
import org.http4s.ContentCoding
import org.http4s.headers.{`Content-Encoding`, `Content-Length`}
import me.ivovk.connect_rpc_scala.ConnectException

object Compressor {
  private[connect_rpc_scala] val supportedCompressions =
    NonEmptyList.of(ContentCoding.gzip, ContentCoding.deflate)
}

class Compressor[F[_]: Sync] {

  given Compression[F] = Compression.forSync[F]

  def decompress(encoding: ContentCoding): Pipe[F, Byte, Byte] = in =>
    encoding match {
      case ContentCoding.gzip =>
        in.through(Compression[F].gunzip().andThen(_.flatMap(_.content)))
      case ContentCoding.deflate =>
        in.through(Compression[F].inflate(InflateParams.DEFAULT))
      case ContentCoding.identity =>
        in
      case other =>
        Stream.raiseError[F](unsupportedCompression(other))
    }

  def compress(encoding: ContentCoding)(entity: EncodedEntity[F]): F[EncodedEntity[F]] =
    if encoding == ContentCoding.identity then Sync[F].pure(entity)
    else
      Sync[F].fromEither(compressionPipe(encoding).map { pipe =>
        EncodedEntity(
          headers = entity.headers
            .transform(_.filterNot(_.name == `Content-Length`.name))
            .put(`Content-Encoding`(encoding)),
          body = entity.body.through(pipe),
        )
      })

  private[codec] def compressionPipe(encoding: ContentCoding): Either[ConnectException, Pipe[F, Byte, Byte]] =
    encoding match {
      case ContentCoding.gzip    => Right(Compression[F].gzip())
      case ContentCoding.deflate => Right(Compression[F].deflate(DeflateParams.DEFAULT))
      case other                 => Left(unsupportedCompression(other))
    }

  private def unsupportedCompression(encoding: ContentCoding): ConnectException =
    ConnectException(
      connectrpc.Code.Unimplemented,
      s"Unsupported compression ${encoding.coding}. Supported encodings are identity, gzip, and deflate.",
    )

}
