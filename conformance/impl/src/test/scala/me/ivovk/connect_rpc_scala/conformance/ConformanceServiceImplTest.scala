package me.ivovk.connect_rpc_scala.conformance

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.protobuf.ByteString
import connectrpc.conformance.v1.*
import org.http4s.{Header as HttpHeader, Headers}
import me.ivovk.connect_rpc_scala.{ConnectException, ConnectRequestContext, ConnectStream}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*

import scala.concurrent.duration.*

class ConformanceServiceImplTest extends AnyFunSuite with Matchers {

  private val service = ConformanceServiceImpl[IO]()

  test("preserves unary headers, trailers, timeout, and request details") {
    val responseDefinition = UnaryResponseDefinition(
      responseHeaders = Seq(Header("response-header", Seq("header-value"))),
      response = UnaryResponseDefinition.Response.ResponseData(ByteString.copyFromUtf8("response")),
      responseTrailers = Seq(Header("response-trailer", Seq("trailer-value"))),
    )
    val message = UnaryRequest(Some(responseDefinition), ByteString.copyFromUtf8("request"))
    val context = ConnectRequestContext(
      Headers(HttpHeader.Raw(ci"request-header", "request-value")),
      Some(250.millis),
    )

    val response = service.unary(message, context).unsafeRunSync()

    response.message.getPayload.data.toStringUtf8 shouldBe "response"
    response.headers.get(ci"response-header").map(_.toList.map(_.value)) shouldBe Some(List("header-value"))
    response.trailers.get(ci"trailer-response-trailer").map(_.toList.map(_.value)) shouldBe Some(
      List("trailer-value")
    )

    val requestInfo = response.message.getPayload.getRequestInfo
    requestInfo.timeoutMs shouldBe Some(250L)
    requestInfo.requestHeaders should contain(Header("request-header", Seq("request-value")))
    requestInfo.requests.head.unpack[UnaryRequest] shouldBe message
  }

  test("returns structured Connect errors with request details") {
    val responseDefinition = UnaryResponseDefinition(
      response = UnaryResponseDefinition.Response.Error(
        Error(Code.InvalidArgument, Some("invalid request"))
      )
    )
    val request = UnaryRequest(Some(responseDefinition))

    val error = service.unary(request, ConnectRequestContext()).attempt.unsafeRunSync() match {
      case Left(value: ConnectException) => value
      case other                         => fail(s"Expected ConnectException, got $other")
    }

    error.code shouldBe connectrpc.Code.InvalidArgument
    error.getMessage shouldBe "invalid request"
    error.details should have size 1
    ConformancePayload.RequestInfo.parseFrom(error.details.head.value.newCodedInput()).requests.head
      .unpack[UnaryRequest] shouldBe request
  }

  test("streams responses and completes with terminal trailers") {
    val responseDefinition = StreamResponseDefinition(
      responseHeaders = Seq(Header("response-header", Seq("header-value"))),
      responseData = Seq(ByteString.copyFromUtf8("first"), ByteString.copyFromUtf8("second")),
      responseTrailers = Seq(Header("response-trailer", Seq("trailer-value"))),
    )
    val request = ServerStreamRequest(Some(responseDefinition))

    val result = service.serverStream(request, ConnectRequestContext()).use { response =>
      response.stream.events.compile.toList.map(response.headers -> _)
    }.unsafeRunSync()

    result._1.get(ci"response-header").map(_.toList.map(_.value)) shouldBe Some(List("header-value"))
    result._2.collect { case ConnectStream.Event.Message(message) =>
      message.getPayload.data.toStringUtf8
    } shouldBe List("first", "second")
    result._2.last shouldBe ConnectStream.Event.End(
      Headers(HttpHeader.Raw(ci"response-trailer", "trailer-value"))
    )
    result._2.collectFirst { case ConnectStream.Event.Message(message) =>
      message.getPayload.getRequestInfo.requests.head.unpack[ServerStreamRequest]
    } shouldBe Some(request)
  }

  test("raises a requested server-streaming error after emitted responses") {
    val responseDefinition = StreamResponseDefinition(
      responseData = Seq(ByteString.copyFromUtf8("first")),
      error = Some(Error(Code.Unavailable, Some("stream failed"))),
      responseTrailers = Seq(Header("response-trailer", Seq("trailer-value"))),
    )
    val request = ServerStreamRequest(Some(responseDefinition))

    val events = service.serverStream(request, ConnectRequestContext()).use { response =>
      response.stream.events.attempt.compile.toList
    }.unsafeRunSync()

    events.head should matchPattern {
      case Right(ConnectStream.Event.Message(ServerStreamResponse(Some(ConformancePayload(data, _)))))
          if data.toStringUtf8 == "first" =>
    }
    val error = events.last.swap.toOption.get.asInstanceOf[ConnectException]
    error.code shouldBe connectrpc.Code.Unavailable
    error.trailers.get(ci"response-trailer").map(_.toList.map(_.value)) shouldBe Some(List("trailer-value"))
  }
}
