package org.ivovk.connect_rpc_scala.grpc

import org.scalatest.funsuite.AnyFunSuite
import test.MergingBuilderTest.EntityToMerge1

class MergingBuilderTest extends AnyFunSuite {

  import MergingBuilder.*

  test("merges three messages") {
    val merge = EntityToMerge1(a = 1)
      .merge(EntityToMerge1(b = 2))
      .merge(EntityToMerge1(a = 3))
      .build

    assert(merge.a == 3)
    assert(merge.b == 2)
  }
}
