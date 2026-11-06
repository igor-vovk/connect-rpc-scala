package org.ivovk.connect_rpc_scala.http4s.headers

import org.http4s.{Header, Headers}
import org.ivovk.connect_rpc_scala.service.ConnectHeaders
import org.typelevel.ci.CIString

import java.util.Locale

private[http4s] object HeaderConverters {

  private val TrailerPrefix       = "trailer-"
  private val TrailerPrefixLength = TrailerPrefix.length

  def toHttp4s(headers: ConnectHeaders): Headers =
    toHttp4s(headers, prefix = "")

  def toHttp4sTrailers(headers: ConnectHeaders): Headers =
    toHttp4s(headers, prefix = TrailerPrefix)

  def toConnect(headers: Headers): ConnectHeaders =
    ConnectHeaders.create(toPairs(headers))

  def toConnectWithTrailers(headers: Headers): (ConnectHeaders, ConnectHeaders) = {
    val (trailers, normal) = toPairs(headers).partition { (name, _) =>
      name.toLowerCase(Locale.ROOT).startsWith(TrailerPrefix)
    }
    val connectTrailers = trailers.map((name, value) => name.drop(TrailerPrefixLength) -> value)
    ConnectHeaders.create(normal) -> ConnectHeaders.create(connectTrailers)
  }

  private def toHttp4s(headers: ConnectHeaders, prefix: String): Headers = {
    val raw = headers.entries.toSeq.flatMap { (name, values) =>
      values.map(value => Header.Raw(CIString(s"$prefix$name"), value))
    }
    new Headers(raw.toList)
  }

  private def toPairs(headers: Headers): Seq[(String, String)] =
    headers.headers.map(header => header.name.toString -> header.value)
}
