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

    val any  = connectrpc.ErrorDetailsAny("type", ByteString.copyFrom(Array[Byte](1, 2, 3)))
    val json = printer.print(any)
    assert(json == """{"type":"type","value":"AQID"}""")

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

    val error = connectrpc.Error(connectrpc.Code.FailedPrecondition, Some("message"), Seq.empty)
    val json  = printer.print(error)
    assert(json == """{"code":"failed_precondition","message":"message"}""")

    val parsed = parser.fromJsonString[connectrpc.Error](json)
    assert(parsed == error)
  }

  test("EndStreamMessage serialization empty") {
    val formatRegistry = json4s.JsonFormat.DefaultRegistry
      .registerMessageFormatter[connectrpc.EndStreamMessage](
        EndStreamMessageFormat.writer,
        EndStreamMessageFormat.parser,
      )

    val parser  = new json4s.Parser().withFormatRegistry(formatRegistry)
    val printer = new json4s.Printer().withFormatRegistry(formatRegistry)

    val message = connectrpc.EndStreamMessage(error = None, metadata = Seq.empty)
    val json    = printer.print(message)
    assert(json == "{}")

    val parsed = parser.fromJsonString[connectrpc.EndStreamMessage](json)
    assert(parsed == message)
  }

  test("EndStreamMessage serialization with error") {
    val formatRegistry = json4s.JsonFormat.DefaultRegistry
      .registerMessageFormatter[connectrpc.Error](
        ConnectErrorFormat.writer,
        ConnectErrorFormat.parser,
      )
      .registerMessageFormatter[connectrpc.EndStreamMessage](
        EndStreamMessageFormat.writer,
        EndStreamMessageFormat.parser,
      )

    val parser  = new json4s.Parser().withFormatRegistry(formatRegistry)
    val printer = new json4s.Printer().withFormatRegistry(formatRegistry)

    val message = connectrpc.EndStreamMessage(
      error = Some(connectrpc.Error(connectrpc.Code.InvalidArgument, Some("invalid input"), Seq.empty)),
      metadata = Seq.empty,
    )
    val json = printer.print(message)
    assert(json == """{"error":{"code":"invalid_argument","message":"invalid input"}}""")

    val parsed = parser.fromJsonString[connectrpc.EndStreamMessage](json)
    assert(parsed == message)
  }

  test("EndStreamMessage serialization with metadata") {
    val formatRegistry = json4s.JsonFormat.DefaultRegistry
      .registerMessageFormatter[connectrpc.EndStreamMessage](
        EndStreamMessageFormat.writer,
        EndStreamMessageFormat.parser,
      )

    val parser  = new json4s.Parser().withFormatRegistry(formatRegistry)
    val printer = new json4s.Printer().withFormatRegistry(formatRegistry)

    val message = connectrpc.EndStreamMessage(
      error = None,
      metadata = Seq(
        connectrpc.MetadataEntry("content-type", Seq("application/json")),
        connectrpc.MetadataEntry("x-custom", Seq("value1", "value2")),
      ),
    )
    val json = printer.print(message)
    assert(json == """{"metadata":{"content-type":["application/json"],"x-custom":["value1","value2"]}}""")

    val parsed = parser.fromJsonString[connectrpc.EndStreamMessage](json)
    assert(parsed == message)
  }

}
