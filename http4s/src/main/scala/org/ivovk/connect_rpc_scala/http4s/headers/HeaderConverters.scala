package org.ivovk.connect_rpc_scala.http4s.headers

import org.http4s.{Header, Headers}
import org.ivovk.connect_rpc_scala.service.ConnectHeaders
import org.typelevel.ci.CIString

import java.util.Locale

private[http4s] object HeaderConverters {

  private val TrailerPrefix       = "trailer-"
  private val TrailerPrefixLength = TrailerPrefix.length

  extension (headers: ConnectHeaders) {
    def toHttp4s: Headers =
      convertToHttp4s(prefix = "")

    def toHttp4sTrailers: Headers =
      convertToHttp4s(prefix = TrailerPrefix)

    private def convertToHttp4s(prefix: String): Headers = {
      val raw = headers.entries.toSeq.flatMap { (name, values) =>
        values.map(value => Header.Raw(CIString(s"$prefix$name"), value))
      }
      new Headers(raw.toList)
    }
  }

  extension (headers: Headers) {
    def toConnect: ConnectHeaders =
      ConnectHeaders.create(toPairs)

    def toConnectWithTrailers: (ConnectHeaders, ConnectHeaders) = {
      val (trailers, normal) = toPairs.partition { (name, _) =>
        name.toLowerCase(Locale.ROOT).startsWith(TrailerPrefix)
      }
      val connectTrailers = trailers.map((name, value) => name.drop(TrailerPrefixLength) -> value)
      ConnectHeaders.create(normal) -> ConnectHeaders.create(connectTrailers)
    }

    private def toPairs: Seq[(String, String)] =
      headers.headers.map(header => header.name.toString -> header.value)
  }

}
