package me.ivovk.connect_rpc_scala.http.codec

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import me.ivovk.connect_rpc_scala.http.MediaTypes
import me.ivovk.connect_rpc_scala.*
import org.http4s.{Header, Headers}
import org.typelevel.ci.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StreamingMessageCodecTest extends AnyFunSuite with Matchers {

  private val jsonSerdes = JsonSerdesBuilder[IO]().build
  private val jsonCodec  = new StreamingMessageCodec(
    jsonSerdes.codec,
    jsonSerdes,
    MediaTypes.`application/connect+json`,
  )

  test("round-trips JSON messages and terminal trailers across arbitrary chunks") {
    val messages = Stream(
      connectrpc.Error(connectrpc.Code.InvalidArgument, Some("first")),
      connectrpc.Error(connectrpc.Code.Unavailable, Some("second")),
    )
    val trailers = Headers(Header.Raw(ci"x-result", "complete"))
    val encoded  = jsonCodec.encodeResponse(
      ConnectStreamingResponse(ConnectStream.fromStream(messages, trailers))
    )
    val fragmented = encoded.body.chunkN(1).unchunks
    val decoded    = jsonCodec.decodeResponse[connectrpc.Error](fragmented, Headers.empty)

    decoded.events.compile.toList.unsafeRunSync() shouldBe List(
      ConnectStream.Event.Message(connectrpc.Error(connectrpc.Code.InvalidArgument, Some("first"))),
      ConnectStream.Event.Message(connectrpc.Error(connectrpc.Code.Unavailable, Some("second"))),
      ConnectStream.Event.End(trailers),
    )
  }

  test("round-trips protobuf streaming requests across arbitrary chunks") {
    val codec = new StreamingMessageCodec(
      new ProtoMessageCodec[IO],
      jsonSerdes,
      MediaTypes.`application/connect+proto`,
    )
    val expected = Seq(
      connectrpc.Error(connectrpc.Code.InvalidArgument, Some("first")),
      connectrpc.Error(connectrpc.Code.Unavailable, Some("second")),
    )
    val encoded    = codec.encodeRequest(Stream.emits(expected))
    val fragmented = encoded.body.chunkN(1).unchunks

    codec.decodeRequest[connectrpc.Error](fragmented, Headers.empty)
      .compile.toList.unsafeRunSync() shouldBe expected
  }

  test("turns a failure after emitted messages into an error end-stream envelope") {
    val expected = connectrpc.Error(connectrpc.Code.InvalidArgument, Some("first"))
    val failure  = ConnectException(
      connectrpc.Code.Unavailable,
      "generation failed",
      trailers = Headers(Header.Raw(ci"x-error", "trailer")),
    )
    val messages = Stream.emit(expected) ++ Stream.raiseError[IO](failure)
    val encoded  = jsonCodec.encodeResponse(
      ConnectStreamingResponse(ConnectStream.fromStream(messages))
    )
    val decoded = jsonCodec.decodeResponse[connectrpc.Error](encoded.body, Headers.empty)
    val events  = decoded.events.attempt.compile.toList.unsafeRunSync()

    events.head shouldBe Right(ConnectStream.Event.Message(expected))
    val decodedFailure = events.last.swap.toOption.get.asInstanceOf[ConnectException]
    decodedFailure.code shouldBe connectrpc.Code.Unavailable
    decodedFailure.getMessage shouldBe "generation failed"
    decodedFailure.trailers.get(ci"x-error").map(_.toList.map(_.value)) shouldBe Some(List("trailer"))
  }

  test("reports truncated request and response envelopes with direction-specific errors") {
    val truncatedHeader  = Array[Byte](0, 0, 0)
    val truncatedPayload = Array[Byte](0, 0, 0, 0, 2, 'x'.toByte)

    for (bytes <- List(truncatedHeader, truncatedPayload)) {
      val body    = Stream.emits(bytes)
      val request = intercept[ConnectException] {
        jsonCodec.decodeRequest[connectrpc.Error](body, Headers.empty).compile.drain.unsafeRunSync()
      }
      val response = intercept[ConnectException] {
        jsonCodec.decodeResponse[connectrpc.Error](body, Headers.empty).events.compile.drain.unsafeRunSync()
      }

      request.code shouldBe connectrpc.Code.InvalidArgument
      response.code shouldBe connectrpc.Code.Internal
    }
  }
}
