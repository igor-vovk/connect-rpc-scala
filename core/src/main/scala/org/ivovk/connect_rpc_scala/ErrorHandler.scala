package org.ivovk.connect_rpc_scala

import org.http4s.Response
import org.ivovk.connect_rpc_scala.http.codec.MessageCodec

trait ErrorHandler[F[_]] {
  def handle(e: Throwable)(using MessageCodec[F]): F[Response[F]]
}
