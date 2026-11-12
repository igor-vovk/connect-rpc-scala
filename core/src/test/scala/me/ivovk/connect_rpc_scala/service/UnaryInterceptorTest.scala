package me.ivovk.connect_rpc_scala.service

import cats.Id
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class UnaryInterceptorTest extends AnyFunSuite with Matchers {

  private val descriptor = UnaryMethodDescriptor(
    serviceName = "connectrpc.TestService",
    methodName = "Call",
    requestCompanion = connectrpc.Error,
    responseCompanion = connectrpc.Error,
  )

  private val request = connectrpc.Error()

  test("identity forwards the context unchanged") {
    val interceptor = UnaryInterceptor.identity[Id, String]

    val response = interceptor.intercept(
      descriptor,
      request,
      "context",
      context => ConnectResponse(request.withMessage(context)),
    )

    response.message.getMessage shouldBe "context"
  }

  test("transformContext transforms the context before execution") {
    val interceptor = UnaryInterceptor.transformContext[Id, String, Int](_.length)

    val response = interceptor.intercept(
      descriptor,
      request,
      "context",
      context => ConnectResponse(request.withMessage(context.toString)),
    )

    response.message.getMessage shouldBe "7"
  }

  test("tapError observes and preserves failures") {
    val failure                     = RuntimeException("failed")
    var observed: Option[Throwable] = None
    val interceptor                 = UnaryInterceptor.tapError[IO, String](error =>
      IO {
        observed = Some(error)
      }
    )

    val result = interceptor
      .intercept(
        descriptor,
        request,
        "context",
        _ => IO.raiseError(failure),
      )
      .attempt
      .unsafeRunSync()

    result shouldBe Left(failure)
    observed shouldBe Some(failure)
  }

  test("tapError does not run for successful responses") {
    var called      = false
    val interceptor = UnaryInterceptor.tapError[IO, String](_ =>
      IO {
        called = true
      }
    )

    interceptor
      .intercept(
        descriptor,
        request,
        "context",
        _ => IO.pure(ConnectResponse(request)),
      )
      .unsafeRunSync()

    called shouldBe false
  }

}
