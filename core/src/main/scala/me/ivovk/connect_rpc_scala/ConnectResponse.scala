package me.ivovk.connect_rpc_scala

import org.http4s.Headers

final case class ConnectResponse[A](
  message: A,
  headers: Headers = Headers.empty,
  trailers: Headers = Headers.empty,
)
