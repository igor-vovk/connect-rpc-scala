package me.ivovk.connect_rpc_scala.http.headers

import org.http4s.{ContentCoding, Header, Headers}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*

class ConnectContentEncodingTest extends AnyFunSuite with Matchers {

  test("renders a typed Connect-Content-Encoding header and reads it case-insensitively") {
    val encoding = `Connect-Content-Encoding`(ContentCoding.gzip)

    Headers(encoding).headers shouldBe List(Header.Raw(ci"Connect-Content-Encoding", "gzip"))
    Headers(Header.Raw(ci"connect-content-encoding", "GZIP")).get[`Connect-Content-Encoding`] shouldBe
      Some(encoding)
  }

  test("rejects malformed content codings") {
    `Connect-Content-Encoding`.parse("gzip, identity").isLeft shouldBe true
  }
}
