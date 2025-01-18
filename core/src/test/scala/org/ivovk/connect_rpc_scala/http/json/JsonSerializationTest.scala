package org.ivovk.connect_rpc_scala.http.json

import com.google.protobuf.ByteString
import org.scalatest.funsuite.AnyFunSuite
import scalapb.json4s

class JsonSerializationTest extends AnyFunSuite {

  test("ErrorDetailsAny serialization") {
    val formatRegistry = json4s.JsonFormat.DefaultRegistry
      .registerMessageFormatter[connectrpc.ErrorDetailsAny](
        ErrorDetailsAnyFormat.writer,
        ErrorDetailsAnyFormat.parser,
      )

    val parser  = new json4s.Parser().withFormatRegistry(formatRegistry)
    val printer = new json4s.Printer().withFormatRegistry(formatRegistry)

    val any    = connectrpc.ErrorDetailsAny("type", ByteString.copyFrom(Array[Byte](1, 2, 3)))
    val json   = printer.print(any)
    val parsed = parser.fromJsonString[connectrpc.ErrorDetailsAny](json)

    assert(parsed == any)
  }

  test("Error serialization") {
    val formatRegistry = json4s.JsonFormat.DefaultRegistry
      .registerMessageFormatter[connectrpc.Error](
        ConnectErrorFormat.writer,
        ConnectErrorFormat.parser,
      )

    val parser  = new json4s.Parser().withFormatRegistry(formatRegistry)
    val printer = new json4s.Printer().withFormatRegistry(formatRegistry)

    val error  = connectrpc.Error(connectrpc.Code.FailedPrecondition, Some("message"), Seq.empty)
    val json   = printer.print(error)
    val parsed = parser.fromJsonString[connectrpc.Error](json)

    assert(parsed == error)
  }
}
