package org.ivovk.connect_rpc_scala.http.json

import com.google.protobuf.ByteString
import org.scalatest.funsuite.AnyFunSuite
import scalapb_circe.{JsonFormat, Parser, Printer}

class JsonSerializationTest extends AnyFunSuite {

  test("ErrorDetailsAny serialization") {
    val formatRegistry = JsonFormat.DefaultRegistry
      .registerMessageFormatter[connectrpc.ErrorDetailsAny](
        ErrorDetailsAnyFormat.writer,
        ErrorDetailsAnyFormat.parser,
      )

    val parser  = new Parser(formatRegistry = formatRegistry)
    val printer = new Printer(formatRegistry = formatRegistry)

    val any  = connectrpc.ErrorDetailsAny("type", ByteString.copyFrom(Array[Byte](1, 2, 3)))
    val json = printer.print(any)
    assert(json == """{"type":"type","value":"AQID"}""")

    val parsed = parser.fromJsonString[connectrpc.ErrorDetailsAny](json)
    assert(parsed == any)
  }

  test("Error serialization") {
    val formatRegistry = JsonFormat.DefaultRegistry
      .registerMessageFormatter[connectrpc.Error](
        ConnectErrorFormat.writer,
        ConnectErrorFormat.parser,
      )

    val parser  = new Parser(formatRegistry = formatRegistry)
    val printer = new Printer(formatRegistry = formatRegistry)

    val error = connectrpc.Error(connectrpc.Code.FailedPrecondition, Some("message"), Seq.empty)
    val json  = printer.print(error)
    assert(json == """{"code":"failed_precondition","message":"message"}""")

    val parsed = parser.fromJsonString[connectrpc.Error](json)
    assert(parsed == error)
  }

}
