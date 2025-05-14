package org.ivovk.connect_rpc_scala.http

import org.scalatest.funsuite.AnyFunSuiteLike

class HeaderMappingTest extends AnyFunSuiteLike {

  import HeaderMapping.*

  test("Default incoming headers filter") {
    // Filter out headers that start with "Connection" or "connection"
    assert(DefaultIncomingHeadersFilter("Connection") === false)
    assert(DefaultIncomingHeadersFilter("connection") === false)
    assert(DefaultIncomingHeadersFilter("connection-keep-alive") === false)

    // Valid headers
    assert(DefaultIncomingHeadersFilter("Content-Type") === true)
  }

  test("Default outgoing headers filter") {
    // Filter out headers that start with "grpc-"
    assert(DefaultOutgoingHeadersFilter("grpc-status") === false)

    // Valid headers
    assert(DefaultOutgoingHeadersFilter("Content-Type") === true)
  }

}
