package org.ivovk.connect_rpc_scala.service

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ConnectHeadersTest extends AnyFunSuite with Matchers {

  test("normalizes header names") {
    val headers = ConnectHeaders.empty.add("Content-Type", "application/json")

    headers.get("content-type") shouldBe Some(Seq("application/json"))
    headers.get("CONTENT-TYPE") shouldBe Some(Seq("application/json"))
  }

  test("preserves repeated header values in insertion order") {
    val headers = ConnectHeaders.empty
      .add("x-value", "first")
      .add("X-Value", "second")

    headers.get("x-value") shouldBe Some(Seq("first", "second"))
    headers.entries.toSeq shouldBe Seq("x-value" -> Seq("first", "second"))
  }

  test("constructs headers from pairs") {
    val headers = ConnectHeaders.fromPairs(
      Seq(
        "x-first"  -> "one",
        "x-second" -> "two",
        "x-first"  -> "three",
      )
    )

    headers.get("x-first") shouldBe Some(Seq("one", "three"))
    headers.get("x-second") shouldBe Some(Seq("two"))
  }
}
