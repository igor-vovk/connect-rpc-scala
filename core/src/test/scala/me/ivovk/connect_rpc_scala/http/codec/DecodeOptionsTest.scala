package me.ivovk.connect_rpc_scala.http.codec

import org.http4s.headers.`Content-Encoding`
import org.http4s.{ContentCoding, Header, Headers}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*

import java.nio.charset.StandardCharsets

class DecodeOptionsTest extends AnyFunSuite with Matchers {

  test("default to UTF-8 and identity when payload headers are absent") {
    val options = DecodeOptions.fromHeaders(Headers.empty)

    options shouldBe DecodeOptions(StandardCharsets.UTF_8, ContentCoding.identity)
  }

  test("use the charset and encoding declared in the headers") {
    val headers = Headers(
      Header.Raw(ci"Content-Type", "application/json; charset=ISO-8859-1"),
      `Content-Encoding`(ContentCoding.gzip),
    )

    DecodeOptions.fromHeaders(headers) shouldBe DecodeOptions(StandardCharsets.ISO_8859_1, ContentCoding.gzip)
  }
}
