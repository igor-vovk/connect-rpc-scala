package me.ivovk.connect_rpc_scala.http.headers

import org.http4s.{ContentCoding, Header, Headers}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*

class ConnectAcceptEncodingTest extends AnyFunSuite with Matchers {

  test("renders codings in preference order and parses the header case-insensitively") {
    val encoding = `Connect-Accept-Encoding`(ContentCoding.deflate, ContentCoding.gzip)

    Headers(encoding).headers shouldBe List(Header.Raw(ci"Connect-Accept-Encoding", "deflate, gzip"))
    Headers(Header.Raw(ci"connect-accept-encoding", "DEFLATE, gzip"))
      .get[`Connect-Accept-Encoding`] shouldBe Some(encoding)
  }

  test("combines repeated fields in preference order") {
    val headers = Headers(
      Header.Raw(ci"Connect-Accept-Encoding", "deflate, gzip"),
      Header.Raw(ci"Connect-Accept-Encoding", "identity"),
    )

    headers.get[`Connect-Accept-Encoding`] shouldBe
      Some(`Connect-Accept-Encoding`(ContentCoding.deflate, ContentCoding.gzip, ContentCoding.identity))
  }
}
