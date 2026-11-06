package org.ivovk.connect_rpc_scala.service

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ConnectExceptionTest extends AnyFunSuite with Matchers {

  test("retains an underlying cause outside the protocol model") {
    val cause     = IllegalArgumentException("invalid input")
    val exception = ConnectException(connectrpc.Code.InvalidArgument, "invalid request").withCause(cause)

    exception.getCause shouldBe cause
    exception.productIterator.toSeq should not contain cause
  }
}
