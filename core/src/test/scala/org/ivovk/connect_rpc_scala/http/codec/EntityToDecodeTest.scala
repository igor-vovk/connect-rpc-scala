package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.IO
import org.ivovk.connect_rpc_scala.service.ConnectHeaders
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

class EntityToDecodeTest extends AnyFunSuite with Matchers {

  test("default to UTF-8 when the content type has no charset") {
    val entity = EntityToDecode[IO]("", ConnectHeaders.empty)

    entity.charset shouldBe StandardCharsets.UTF_8
  }

  test("use the charset declared by the content type") {
    val headers = ConnectHeaders.empty.add("content-type", "application/json; charset=ISO-8859-1")
    val entity  = EntityToDecode[IO]("", headers)

    entity.charset shouldBe StandardCharsets.ISO_8859_1
  }
}
