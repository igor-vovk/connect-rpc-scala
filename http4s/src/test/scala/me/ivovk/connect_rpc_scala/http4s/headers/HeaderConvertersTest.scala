package me.ivovk.connect_rpc_scala.http4s.headers

import org.http4s.{Header, Headers}
import me.ivovk.connect_rpc_scala.http4s.headers.HeaderConverters.*
import me.ivovk.connect_rpc_scala.service.ConnectHeaders
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*

class HeaderConvertersTest extends AnyFunSuite with Matchers {

  test("convert Connect headers to http4s headers") {
    val connectHeaders = ConnectHeaders.create(
      Seq(
        "x-value" -> "first",
        "x-value" -> "second",
        "x-other" -> "other",
      )
    )

    val headers = connectHeaders.toHttp4s

    values(headers, ci"x-value") shouldBe Seq("first", "second")
    values(headers, ci"x-other") shouldBe Seq("other")
  }

  test("convert http4s headers to Connect headers") {
    val headers = new Headers(
      List(
        Header.Raw(ci"X-Value", "first"),
        Header.Raw(ci"x-value", "second"),
        Header.Raw(ci"X-Other", "other"),
      )
    )

    val connectHeaders = headers.toConnect

    connectHeaders.get("x-value") shouldBe Some(Seq("first", "second"))
    connectHeaders.get("X-OTHER") shouldBe Some(Seq("other"))
  }

  test("prefix Connect trailers when converting to http4s headers") {
    val trailers = ConnectHeaders.create(
      Seq(
        "x-value" -> "first",
        "x-value" -> "second",
      )
    )

    val headers = trailers.toHttp4sTrailers

    values(headers, ci"trailer-x-value") shouldBe Seq("first", "second")
    values(headers, ci"x-value") shouldBe empty
  }

  test("separate prefixed trailers from http4s headers") {
    val headers = new Headers(
      List(
        Header.Raw(ci"X-Header", "header-value"),
        Header.Raw(ci"Trailer-X-Trailer", "first"),
        Header.Raw(ci"TRAILER-X-TRAILER", "second"),
      )
    )

    val (connectHeaders, connectTrailers) = headers.toConnectWithTrailers

    connectHeaders.get("x-header") shouldBe Some(Seq("header-value"))
    connectHeaders.get("trailer-x-trailer") shouldBe None
    connectTrailers.get("x-trailer") shouldBe Some(Seq("first", "second"))
  }

  private def values(headers: Headers, name: CIString): Seq[String] =
    headers.headers.collect { case header if header.name == name => header.value }
}
