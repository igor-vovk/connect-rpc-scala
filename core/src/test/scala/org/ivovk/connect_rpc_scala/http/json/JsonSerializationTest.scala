package org.ivovk.connect_rpc_scala.http.json

import com.google.protobuf.ByteString
import org.scalatest.funsuite.AnyFunSuite
import scalapb.json4s

class JsonSerializationTest extends AnyFunSuite {

  test("ErrorDetailsAny serialization") {
    val formatRegistry = json4s.JsonFormat.DefaultRegistry
      .registerMessageFormatter[connectrpc.ErrorDetailsAny](
        ErrorDetailsAnyFormat.writer,
        ErrorDetailsAnyFormat.printer
      )

    val parser = new json4s.Parser().withFormatRegistry(formatRegistry)
    val printer = new json4s.Printer().withFormatRegistry(formatRegistry)

    val any = connectrpc.ErrorDetailsAny("type", ByteString.copyFrom(Array[Byte](1, 2, 3)))
    val json = printer.print(any)
    val parsed = parser.fromJsonString[connectrpc.ErrorDetailsAny](json)

    assert(parsed == any)
  }
}
