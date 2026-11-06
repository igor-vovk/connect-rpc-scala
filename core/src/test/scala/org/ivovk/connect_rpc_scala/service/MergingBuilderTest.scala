package org.ivovk.connect_rpc_scala.service

import connectrpc.{Code, Error}
import org.scalatest.funsuite.AnyFunSuite

class MergingBuilderTest extends AnyFunSuite {

  import MergingBuilder.*

  test("merges protobuf messages without a transport dependency") {
    val merged = Error(code = Code.Internal)
      .merge(Error(message = Some("message")))
      .build

    assert(merged.code == Code.Internal)
    assert(merged.message.contains("message"))
  }
}
