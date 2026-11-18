package me.ivovk.connect_rpc_scala.http.codec

import cats.effect.IO
import org.http4s.{Header, Headers}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*

import java.nio.charset.StandardCharsets

class EntityToDecodeTest extends AnyFunSuite with Matchers {

  test("default to UTF-8 when the content type has no charset") {
    val entity = EntityToDecode[IO]("", Headers.empty)

    entity.charset shouldBe StandardCharsets.UTF_8
  }

  test("use the charset declared by the content type") {
    val headers = Headers(Header.Raw(ci"Content-Type", "application/json; charset=ISO-8859-1"))
    val entity  = EntityToDecode[IO]("", headers)

    entity.charset shouldBe StandardCharsets.ISO_8859_1
  }
}
