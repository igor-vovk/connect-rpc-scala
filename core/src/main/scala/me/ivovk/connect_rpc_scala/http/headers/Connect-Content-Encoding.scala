package me.ivovk.connect_rpc_scala.http.headers

import org.http4s.{ContentCoding, Header, ParseFailure, ParseResult}
import org.typelevel.ci.*

private[connect_rpc_scala] object `Connect-Content-Encoding` {

  def parse(value: String): ParseResult[`Connect-Content-Encoding`] =
    ContentCoding.parse(value)
      .left.map(failure => ParseFailure("Invalid Connect-Content-Encoding header", failure.details))
      .map(`Connect-Content-Encoding`(_))

  implicit val headerInstance: Header[`Connect-Content-Encoding`, Header.Single] =
    Header.createRendered(
      ci"Connect-Content-Encoding",
      _.contentCoding,
      parse,
    )
}

private[connect_rpc_scala] final case class `Connect-Content-Encoding`(contentCoding: ContentCoding)
