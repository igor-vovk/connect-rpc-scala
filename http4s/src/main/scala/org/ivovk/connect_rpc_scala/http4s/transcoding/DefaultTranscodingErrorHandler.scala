package org.ivovk.connect_rpc_scala.http4s.transcoding

import cats.Applicative
import cats.implicits.*
import org.http4s.{Response, Status}
import org.ivovk.connect_rpc_scala.http.codec.MessageCodec
import org.ivovk.connect_rpc_scala.http4s.ErrorHandler

class DefaultTranscodingErrorHandler[F[_]: Applicative] extends ErrorHandler[F] {
  override def handle(e: Throwable)(using MessageCodec[F]): F[Response[F]] =
    Response[F](Status.InternalServerError).pure[F]
}
