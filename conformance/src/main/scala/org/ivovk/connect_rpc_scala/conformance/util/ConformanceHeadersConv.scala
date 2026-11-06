package org.ivovk.connect_rpc_scala.conformance.util

import connectrpc.conformance.v1.Header
import org.ivovk.connect_rpc_scala.service.ConnectHeaders

object ConformanceHeadersConv {

  def toHeaderSeq(headers: ConnectHeaders): Seq[Header] =
    headers.entries.toSeq.map { case (name, values) => Header(name, values) }

  def toConnectHeaders(headers: Seq[Header]): ConnectHeaders =
    ConnectHeaders.fromPairs(
      headers.flatMap(header => header.value.map(value => header.name -> value))
    )
}
