package me.ivovk.connect_rpc_scala.http4s.headers

import org.http4s.{Header, Headers}
import me.ivovk.connect_rpc_scala.http4s.headers.UnaryTrailers.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*

class UnaryTrailersTest extends AnyFunSuite with Matchers {

  test("prefix Connect trailers when writing http4s headers") {
    val trailers = Headers(
      Header.Raw(ci"x-value", "first"),
      Header.Raw(ci"X-Value", "second"),
    )

    val headers = trailers.withTrailerPrefix

    values(headers, ci"trailer-x-value") shouldBe Seq("first", "second")
    values(headers, ci"x-value") shouldBe empty
  }

  test("separate prefixed trailers from http4s headers") {
    val headers = Headers(
      Header.Raw(ci"X-Header", "header-value"),
      Header.Raw(ci"Trailer-X-Trailer", "first"),
      Header.Raw(ci"TRAILER-X-TRAILER", "second"),
    )

    val (metadata, trailers) = headers.splitTrailers

    values(metadata, ci"x-header") shouldBe Seq("header-value")
    values(metadata, ci"trailer-x-trailer") shouldBe empty
    values(trailers, ci"x-trailer") shouldBe Seq("first", "second")
  }

  private def values(headers: Headers, name: CIString): Seq[String] =
    headers.headers.collect { case header if header.name == name => header.value }
}
