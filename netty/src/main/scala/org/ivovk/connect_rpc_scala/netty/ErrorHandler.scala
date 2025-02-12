package org.ivovk.connect_rpc_scala.netty

import io.netty.handler.codec.http.HttpResponse
import org.ivovk.connect_rpc_scala.http.codec.MessageCodec

trait ErrorHandler[F[_]] {
  def handle(e: Throwable)(using MessageCodec[F]): F[HttpResponse]
}
