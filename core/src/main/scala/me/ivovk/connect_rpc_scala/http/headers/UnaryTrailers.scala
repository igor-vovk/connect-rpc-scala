package me.ivovk.connect_rpc_scala.http.headers

import org.http4s.{Header, Headers}
import org.typelevel.ci.CIString

import java.util.Locale

private[connect_rpc_scala] object UnaryTrailers {

  private val TrailerPrefix = "trailer-"

  extension (headers: Headers) {
    def withTrailerPrefix: Headers =
      headers.transform(_.map { header =>
        Header.Raw(CIString(s"$TrailerPrefix${header.name}"), header.value)
      })

    def splitTrailers: (Headers, Headers) = {
      val (trailers, normal) = headers.headers.partition { header =>
        header.name.toString.toLowerCase(Locale.ROOT).startsWith(TrailerPrefix)
      }
      val unprefixed = trailers.map { header =>
        Header.Raw(CIString(header.name.toString.drop(TrailerPrefix.length)), header.value)
      }
      Headers(normal) -> Headers(unprefixed)
    }
  }
}
