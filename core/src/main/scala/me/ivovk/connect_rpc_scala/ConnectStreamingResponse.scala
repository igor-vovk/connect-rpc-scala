package me.ivovk.connect_rpc_scala

import org.http4s.Headers

final case class ConnectStreamingResponse[F[_], A](
  stream: ConnectStream[F, A],
  headers: Headers = Headers.empty,
)
