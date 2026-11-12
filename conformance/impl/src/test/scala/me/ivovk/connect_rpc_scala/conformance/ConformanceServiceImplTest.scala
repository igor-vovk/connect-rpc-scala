package me.ivovk.connect_rpc_scala.conformance

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.protobuf.ByteString
import connectrpc.conformance.v1.*
import me.ivovk.connect_rpc_scala.service.{ConnectException, ConnectHeaders, ConnectRequestContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

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
      ConnectHeaders.create(Seq("request-header" -> "request-value")),
      Some(250.millis),
    )

    val response = service.unary(message, context).unsafeRunSync()

    response.message.getPayload.data.toStringUtf8 shouldBe "response"
    response.headers.get("response-header") shouldBe Some(Vector("header-value"))
    response.trailers.get("trailer-response-trailer") shouldBe Some(Vector("trailer-value"))

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
}
