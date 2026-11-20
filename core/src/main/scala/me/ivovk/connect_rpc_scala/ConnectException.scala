package me.ivovk.connect_rpc_scala

import org.http4s.Headers

final case class ConnectException(
  code: connectrpc.Code,
  message: String,
  details: Seq[connectrpc.ErrorDetailsAny] = Seq.empty,
  headers: Headers = Headers.empty,
  trailers: Headers = Headers.empty,
) extends RuntimeException(message) {

  private[connect_rpc_scala] def withCause(cause: Throwable): ConnectException = {
    initCause(cause)
    this
  }
}
