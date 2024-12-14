package org.ivovk.connect_rpc_scala.grpc

import org.scalatest.funsuite.AnyFunSuite
import test.HttpCommunicationTest.AddRequest

class MergingBuilderTest extends AnyFunSuite {
  import MergingBuilder.*

  test("merges three messages") {
    val merge = AddRequest(a = 1).merge(AddRequest(b = 2)).merge(AddRequest(a = 3)).build

    assert(merge.a == 3)
    assert(merge.b == 2)
  }
}
