package org.ivovk.connect_rpc_scala.grpc

import cats.effect.{Deferred, IO}
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.Stream
import io.grpc.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.InputStream

class ClientCallsTest extends AsyncFunSuite with AsyncIOSpec with Matchers {

  private val dummyMarshaller = new MethodDescriptor.Marshaller[String] {
    override def stream(value: String): InputStream = ???
    override def parse(stream: InputStream): String = ???
  }

  // Mock method descriptors for testing
  private val clientStreamingMethodDescriptor = MethodDescriptor
    .newBuilder[String, String]()
    .setType(MethodDescriptor.MethodType.CLIENT_STREAMING)
    .setFullMethodName("test.Service/ClientStreamMethod")
    .setRequestMarshaller(dummyMarshaller)
    .setResponseMarshaller(dummyMarshaller)
    .build()

  private val serverStreamingMethodDescriptor = MethodDescriptor
    .newBuilder[String, String]()
    .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
    .setFullMethodName("test.Service/ServerStreamMethod")
    .setRequestMarshaller(dummyMarshaller)
    .setResponseMarshaller(dummyMarshaller)
    .build()

  // Mock channel that provides a controllable ClientCall
  class TestClientCall extends ClientCall[String, String] {
    var listener: ClientCall.Listener[String] = _
    var requestCount: Int                     = 0
    var halfClosed: Boolean                   = false
    var cancelled: Boolean                    = false
    var sentMessages: List[String]            = List.empty

    private val whenStarted_  = Deferred.unsafe[IO, Unit]
    def whenStarted: IO[Unit] = whenStarted_.get

    override def start(responseListener: ClientCall.Listener[String], headers: Metadata): Unit = {
      listener = responseListener
      whenStarted_.complete(()).unsafeRunSync()
    }

    override def request(numMessages: Int): Unit =
      requestCount = numMessages

    override def cancel(message: String, cause: Throwable): Unit =
      cancelled = true

    override def halfClose(): Unit =
      halfClosed = true

    override def sendMessage(message: String): Unit =
      sentMessages = sentMessages :+ message

    // Helper methods to simulate server responses
    def simulateHeaders(headers: Metadata): Unit =
      listener.onHeaders(headers)

    def simulateMessage(message: String): Unit =
      listener.onMessage(message)

    def simulateClose(status: Status, trailers: Metadata): Unit =
      listener.onClose(status, trailers)
  }

  class TestChannel(testCall: TestClientCall) extends Channel {
    override def newCall[ReqT, RespT](
      methodDescriptor: MethodDescriptor[ReqT, RespT],
      callOptions: CallOptions,
    ): ClientCall[ReqT, RespT] =
      testCall.asInstanceOf[ClientCall[ReqT, RespT]]

    override def authority(): String = "test-authority"
  }

  // ========== clientStreamingCall tests ==========

  test("clientStreamingCall should send all request messages and return single response") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val headers = new Metadata()
    headers.put(Metadata.Key.of("test-header", Metadata.ASCII_STRING_MARSHALLER), "test-value")

    val result = for {
      responseFiber <- ClientCalls.clientStreamingCall[IO, String, String](
        channel,
        clientStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        Stream("msg1", "msg2", "msg3"),
      ).start
      _ <- testCall.whenStarted *> IO {
        testCall.simulateHeaders(headers)
        testCall.simulateMessage("response")
        testCall.simulateClose(Status.OK, new Metadata())
      }
      response <- responseFiber.joinWithNever
    } yield {
      testCall.sentMessages should contain theSameElementsInOrderAs List("msg1", "msg2", "msg3")
      testCall.halfClosed shouldBe true
      testCall.requestCount shouldBe 2

      response.headers shouldBe headers
      response.value shouldBe "response"
    }

    result.asserting(_ => succeed)
  }

  test("clientStreamingCall should handle empty request stream") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val result = for {
      responseFiber <- ClientCalls.clientStreamingCall[IO, String, String](
        channel,
        clientStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        Stream.empty,
      ).start
      _ <- testCall.whenStarted *> IO {
        testCall.simulateHeaders(new Metadata())
        testCall.simulateMessage("response")
        testCall.simulateClose(Status.OK, new Metadata())
      }
      response <- responseFiber.joinWithNever
    } yield {
      testCall.sentMessages shouldBe empty
      testCall.halfClosed shouldBe true
      response.value shouldBe "response"
    }

    result.asserting(_ => succeed)
  }

  test("clientStreamingCall should handle error status") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val result = for {
      responseFiber <- ClientCalls.clientStreamingCall[IO, String, String](
        channel,
        clientStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        Stream("msg1"),
      ).start
      _ <- testCall.whenStarted *> IO {
        testCall.simulateHeaders(new Metadata())
        testCall.simulateClose(Status.INVALID_ARGUMENT.withDescription("Bad request"), new Metadata())
      }
      error <- responseFiber.joinWithNever.attempt
    } yield {
      error.isLeft shouldBe true
      val exception = error.left.toOption.get
      exception shouldBe a[StatusExceptionWithHeaders]
      exception.asInstanceOf[
        StatusExceptionWithHeaders
      ].getStatus.getCode shouldBe Status.INVALID_ARGUMENT.getCode
    }

    result.asserting(_ => succeed)
  }

  test("clientStreamingCall should fail when no response message received") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val result = for {
      responseFiber <- ClientCalls.clientStreamingCall[IO, String, String](
        channel,
        clientStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        Stream("msg1"),
      ).start
      _ <- testCall.whenStarted *> IO {
        testCall.simulateHeaders(new Metadata())
        // Close without sending a message
        testCall.simulateClose(Status.OK, new Metadata())
      }
      error <- responseFiber.joinWithNever.attempt
    } yield {
      error.isLeft shouldBe true
      error.left.toOption.get.getMessage should include("No value received")
    }

    result.asserting(_ => succeed)
  }

  test("clientStreamingCall should fail when multiple response messages received") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val result = for {
      responseFiber <- ClientCalls.clientStreamingCall[IO, String, String](
        channel,
        clientStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        Stream("msg1"),
      ).start
      error <- testCall.whenStarted *> IO {
        testCall.simulateHeaders(new Metadata())
        testCall.simulateMessage("response1")
        // Try to send a second message - should throw
        testCall.simulateMessage("response2")
      }.attempt
    } yield {
      error.isLeft shouldBe true
      error.left.toOption.get.getMessage should include("More than one message received")
    }

    result.asserting(_ => succeed)
  }

  test("clientStreamingCall should propagate headers and trailers") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val headers = new Metadata()
    headers.put(Metadata.Key.of("response-header", Metadata.ASCII_STRING_MARSHALLER), "header-value")

    val trailers = new Metadata()
    trailers.put(Metadata.Key.of("response-trailer", Metadata.ASCII_STRING_MARSHALLER), "trailer-value")

    val result = for {
      responseFiber <- ClientCalls.clientStreamingCall[IO, String, String](
        channel,
        clientStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        Stream("msg1"),
      ).start
      _ <- testCall.whenStarted *> IO {
        testCall.simulateHeaders(headers)
        testCall.simulateMessage("response")
        testCall.simulateClose(Status.OK, trailers)
      }
      response <- responseFiber.joinWithNever
    } yield {
      response.headers.get(
        Metadata.Key.of("response-header", Metadata.ASCII_STRING_MARSHALLER)
      ) shouldBe "header-value"
      response.trailers.get(
        Metadata.Key.of("response-trailer", Metadata.ASCII_STRING_MARSHALLER)
      ) shouldBe "trailer-value"
    }

    result.asserting(_ => succeed)
  }

  test("clientStreamingCall should handle large number of request messages") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val messageCount = 100 // Reduced from 1000 for faster tests
    val messages     = Stream.range(1, messageCount + 1).map(i => s"msg$i")

    val result = for {
      responseFiber <- ClientCalls.clientStreamingCall[IO, String, String](
        channel,
        clientStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        messages,
      ).start
      _ <- testCall.whenStarted *> IO {
        testCall.simulateHeaders(new Metadata())
        testCall.simulateMessage("response")
        testCall.simulateClose(Status.OK, new Metadata())
      }
      response <- responseFiber.joinWithNever
    } yield {
      testCall.sentMessages.size shouldBe messageCount
      testCall.sentMessages.head shouldBe "msg1"
      testCall.sentMessages.last shouldBe s"msg$messageCount"
      response.value shouldBe "response"
    }

    result.asserting(_ => succeed)
  }

  // ========== serverStreamingCall tests ==========

  test("serverStreamingCall should return headers immediately and stream messages") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val headers = new Metadata()
    headers.put(Metadata.Key.of("test-header", Metadata.ASCII_STRING_MARSHALLER), "test-value")

    val result = for {
      responseFiber <- ClientCalls.serverStreamingCall[IO, String, String](
        channel,
        serverStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        Stream.emit("request"),
      ).start
      _ <- testCall.whenStarted *> IO {
        testCall.simulateHeaders(headers)
        testCall.simulateMessage("message1")
        testCall.simulateMessage("message2")
        testCall.simulateMessage("message3")
        testCall.simulateClose(Status.OK, new Metadata())
      }
      response <- responseFiber.joinWithNever
      messages <- response.messages.compile.toList
    } yield {
      response.headers shouldBe headers
      testCall.halfClosed shouldBe true
      testCall.requestCount shouldBe Int.MaxValue

      // Extract actual messages (filtering out trailers)
      val actualMessages = messages.collect { case msg: String if msg.startsWith("message") => msg }
      actualMessages should contain theSameElementsInOrderAs List("message1", "message2", "message3")

      // Verify trailers are at the end
      messages.last shouldBe a[Metadata]
    }

    result.asserting(_ => succeed)
  }

  test("serverStreamingCall should handle error status") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val result = for {
      responseFiber <- ClientCalls.serverStreamingCall[IO, String, String](
        channel,
        serverStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        Stream.emit("request"),
      ).start
      _ <- testCall.whenStarted *> IO {
        testCall.simulateHeaders(new Metadata())
        testCall.simulateMessage("message1")
        testCall.simulateClose(Status.INTERNAL.withDescription("Test error"), new Metadata())
      }
      response <- responseFiber.joinWithNever
      error    <- response.messages.compile.toList.attempt
    } yield {
      error.isLeft shouldBe true
      error.left.toOption.get shouldBe a[StatusException]
      error.left.toOption.get.asInstanceOf[StatusException].getStatus.getCode shouldBe Status.INTERNAL.getCode
    }

    result.asserting(_ => succeed)
  }

  test("serverStreamingCall should handle empty stream") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val result = for {
      responseFiber <- ClientCalls.serverStreamingCall[IO, String, String](
        channel,
        serverStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        Stream.emit("request"),
      ).start
      _ <- testCall.whenStarted *> IO {
        testCall.simulateHeaders(new Metadata())
        testCall.simulateClose(Status.OK, new Metadata())
      }.start
      response <- responseFiber.joinWithNever
      messages <- response.messages.compile.toList
    } yield {
      // Should only have trailers
      messages.size shouldBe 1
      messages.head shouldBe a[Metadata]
    }

    result.asserting(_ => succeed)
  }

  test("serverStreamingCall should handle multiple requests") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val result = for {
      responseFiber <- ClientCalls.serverStreamingCall[IO, String, String](
        channel,
        serverStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        Stream("req1", "req2", "req3"),
      ).start
      _ <- testCall.whenStarted *> IO {
        testCall.simulateHeaders(new Metadata())
        testCall.simulateMessage("resp1")
        testCall.simulateMessage("resp2")
        testCall.simulateClose(Status.OK, new Metadata())
      }.start
      response <- responseFiber.joinWithNever
      messages <- response.messages.compile.toList
    } yield {
      testCall.halfClosed shouldBe true
      testCall.sentMessages should contain theSameElementsInOrderAs List("req1", "req2", "req3")
      messages.size should be >= 2
    }

    result.asserting(_ => succeed)
  }

  test("serverStreamingCall should work when headers are not sent (only trailers)") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val result = for {
      responseFiber <- ClientCalls.serverStreamingCall[IO, String, String](
        channel,
        serverStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        Stream.emit("request"),
      ).start
      _ <- testCall.whenStarted *> IO {
        // Simulate server sending only trailers (no headers, no messages)
        val trailers = new Metadata()
        trailers.put(Metadata.Key.of("trailer-key", Metadata.ASCII_STRING_MARSHALLER), "trailer-value")
        testCall.simulateClose(Status.OK, trailers)
      }
      response <- responseFiber.joinWithNever
      messages <- response.messages.compile.toList
    } yield {
      // Headers should be empty metadata
      response.headers should not be null
      // Should have trailers in the stream
      messages.size shouldBe 1
      messages.head shouldBe a[Metadata]
    }

    result.asserting(_ => succeed)
  }

  test("serverStreamingCall should handle large number of messages") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val messageCount = 100 // Reduced for faster tests

    val result = for {
      responseFiber <- ClientCalls.serverStreamingCall[IO, String, String](
        channel,
        serverStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        Stream.emit("request"),
      ).start
      _ <- testCall.whenStarted *> IO {
        testCall.simulateHeaders(new Metadata())
        (1 to messageCount).foreach(i => testCall.simulateMessage(s"message$i"))
        testCall.simulateClose(Status.OK, new Metadata())
      }
      response <- responseFiber.joinWithNever
      messages <- response.messages.compile.toList
    } yield {
      val actualMessages = messages.collect { case msg: String => msg }
      actualMessages.size shouldBe messageCount
      actualMessages.head shouldBe "message1"
      actualMessages.last shouldBe s"message$messageCount"
    }

    result.asserting(_ => succeed)
  }

  test("serverStreamingCall should propagate trailers metadata") {
    val testCall = new TestClientCall
    val channel  = new TestChannel(testCall)

    val result = for {
      responseFiber <- ClientCalls.serverStreamingCall[IO, String, String](
        channel,
        serverStreamingMethodDescriptor,
        CallOptions.DEFAULT,
        new Metadata(),
        Stream.emit("request"),
      ).start
      _ <- testCall.whenStarted *> IO {
        testCall.simulateHeaders(new Metadata())
        testCall.simulateMessage("message")

        val trailers = new Metadata()
        trailers.put(Metadata.Key.of("custom-trailer", Metadata.ASCII_STRING_MARSHALLER), "trailer-value")
        testCall.simulateClose(Status.OK, trailers)
      }
      response <- responseFiber.joinWithNever
      messages <- response.messages.compile.toList
    } yield {
      val trailerMetadata = messages.last.asInstanceOf[Metadata]
      trailerMetadata.get(
        Metadata.Key.of("custom-trailer", Metadata.ASCII_STRING_MARSHALLER)
      ) shouldBe "trailer-value"
    }

    result.asserting(_ => succeed)
  }
}
