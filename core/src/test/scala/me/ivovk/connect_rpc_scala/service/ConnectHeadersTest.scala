package me.ivovk.connect_rpc_scala.service

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
    val headers = ConnectHeaders.create(
      Seq(
        "x-first"  -> "one",
        "x-second" -> "two",
        "x-first"  -> "three",
      )
    )

    headers.get("x-first") shouldBe Some(Seq("one", "three"))
    headers.get("x-second") shouldBe Some(Seq("two"))
  }

  test("combines headers and appends repeated values") {
    val left = ConnectHeaders.create(
      Seq(
        "x-value" -> "first",
        "x-left"  -> "left",
      )
    )
    val right = ConnectHeaders.create(
      Seq(
        "X-Value" -> "second",
        "x-right" -> "right",
      )
    )

    val combined = left ++ right

    combined.get("x-value") shouldBe Some(Seq("first", "second"))
    combined.get("x-left") shouldBe Some(Seq("left"))
    combined.get("x-right") shouldBe Some(Seq("right"))
  }

  test("combines with empty headers") {
    val headers = ConnectHeaders.create(Seq("x-value" -> "value"))

    headers ++ ConnectHeaders.empty shouldBe headers
    ConnectHeaders.empty ++ headers shouldBe headers
  }
}
