package org.ivovk.connect_rpc_scala.netty

import org.scalatest.funsuite.AnyFunSuiteLike

class HttpServerHandlerTest extends AnyFunSuiteLike {

  import HttpServerHandler.*

  test("extractPathSegments") {
    assert(extractPathSegments("/service/method", Nil) == Right(List("service", "method")))
    assert(extractPathSegments("/service/method", List("service")) == Right(List("method")))
    assert(extractPathSegments("/service/method", List("service", "method")) == Right(Nil))
    assert(
      extractPathSegments("/service/method", List("service", "method", "extra")) == Left(
        PrefixMismatch
      )
    )
  }

}
