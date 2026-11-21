package me.ivovk.connect_rpc_scala.http.headers

import cats.Semigroup
import cats.data.NonEmptyList
import org.http4s.headers.`Accept-Encoding`
import org.http4s.{ContentCoding, Header, ParseFailure, ParseResult}
import org.typelevel.ci.*

private[connect_rpc_scala] object `Connect-Accept-Encoding` {

  def apply(head: ContentCoding, tail: ContentCoding*): `Connect-Accept-Encoding` =
    `Connect-Accept-Encoding`(NonEmptyList(head, tail.toList))

  def parse(value: String): ParseResult[`Connect-Accept-Encoding`] =
    `Accept-Encoding`.parse(value)
      .left.map(failure => ParseFailure("Invalid Connect-Accept-Encoding header", failure.details))
      .map(encoding => `Connect-Accept-Encoding`(encoding.values))

  implicit val headerInstance: Header[`Connect-Accept-Encoding`, Header.Recurring] =
    Header.createRendered(
      ci"Connect-Accept-Encoding",
      _.values,
      parse,
    )

  implicit val headerSemigroupInstance: Semigroup[`Connect-Accept-Encoding`] =
    Semigroup.instance((left, right) => `Connect-Accept-Encoding`(left.values.concatNel(right.values)))
}

private[connect_rpc_scala] final case class `Connect-Accept-Encoding`(values: NonEmptyList[ContentCoding])
