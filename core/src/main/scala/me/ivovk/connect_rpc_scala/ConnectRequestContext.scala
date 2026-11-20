package me.ivovk.connect_rpc_scala

import org.http4s.Headers

import scala.concurrent.duration.FiniteDuration

final case class ConnectRequestContext(
  headers: Headers = Headers.empty,
  timeout: Option[FiniteDuration] = None,
) {
  require(headers.removePayloadHeaders == headers, "Payload headers can not be passed")
}
