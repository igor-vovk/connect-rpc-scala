package org.ivovk.connect_rpc_scala.http

import org.scalatest.funsuite.AnyFunSuiteLike

class PathsTest extends AnyFunSuiteLike {

  import Paths.*

  test("extractPathSegments") {
    assert(extractPathSegments("/service/method").contains(List("service", "method")))
    assert(extractPathSegments("/service/method", List("service")).contains(List("method")))
    assert(extractPathSegments("/service/method", List("service", "method")).contains(Nil))
    assert(extractPathSegments("/service/method", List("service", "method", "extra")).isEmpty)
  }

}
