package org.ivovk.connect_rpc_scala.conformance

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import connectrpc.conformance.v1 as conformance
import fs2.Stream
import org.http4s.ContentCoding
import _root_.org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, JsonSerdesBuilder}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets

class ConformanceJsonCodecTest extends AnyFunSuite {

  private val jsonSerdes = JsonSerdesBuilder[IO]()
    .registerType[conformance.Header]
    .registerType[conformance.Error]
    .registerType[conformance.ConformancePayload]
    .registerType[conformance.ConformancePayload.RequestInfo]
    .registerType[conformance.ConformancePayload.ConnectGetInfo]
    .registerType[conformance.UnaryRequest]
    .registerType[conformance.UnaryResponse]
    .registerType[conformance.IdempotentUnaryRequest]
    .registerType[conformance.IdempotentUnaryResponse]
    .registerType[conformance.ClientStreamRequest]
    .registerType[conformance.ClientStreamResponse]
    .registerType[conformance.ServerStreamRequest]
    .registerType[conformance.ServerStreamResponse]
    .registerType[conformance.BidiStreamRequest]
    .registerType[conformance.BidiStreamResponse]
    .build

  private val options = EncodeOptions(
    charset = StandardCharsets.UTF_8,
    encoding = ContentCoding.identity,
  )

  test("streaming JSON codec can encode Any-packed ServerStreamRequest") {
    val response = conformance.ServerStreamResponse(
      payload = Some(
        conformance.ConformancePayload(
          data = ByteString.copyFromUtf8("test response"),
          requestInfo = Some(
            conformance.ConformancePayload.RequestInfo(
              requests = Seq(
                Any.pack(
                  conformance.ServerStreamRequest(
                    responseDefinition = Some(
                      conformance.StreamResponseDefinition(
                        responseData = Seq(ByteString.copyFromUtf8("test response"))
                      )
                    )
                  )
                )
              )
            )
          ),
        )
      )
    )

    val body = jsonSerdes.streamingCodec
      .encode(Stream.emit(response), options)
      .body
      .compile
      .toVector
      .unsafeRunSync()

    assert(body.nonEmpty)
  }

  test("streaming JSON codec can encode Any-packed BidiStreamRequest") {
    val response = conformance.BidiStreamResponse(
      payload = Some(
        conformance.ConformancePayload(
          data = ByteString.copyFromUtf8("test response"),
          requestInfo = Some(
            conformance.ConformancePayload.RequestInfo(
              requests = Seq(
                Any.pack(
                  conformance.BidiStreamRequest(
                    responseDefinition = Some(
                      conformance.StreamResponseDefinition(
                        responseData = Seq(ByteString.copyFromUtf8("test response"))
                      )
                    ),
                    fullDuplex = true,
                  )
                )
              )
            )
          ),
        )
      )
    )

    val body = jsonSerdes.streamingCodec
      .encode(Stream.emit(response), options)
      .body
      .compile
      .toVector
      .unsafeRunSync()

    assert(body.nonEmpty)
  }
}
