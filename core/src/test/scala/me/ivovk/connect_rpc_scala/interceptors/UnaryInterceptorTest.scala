package me.ivovk.connect_rpc_scala.interceptors

import cats.Id
import cats.effect.{IO, SyncIO}
import cats.effect.unsafe.implicits.global
import me.ivovk.connect_rpc_scala.{
  AttributeReader,
  ConnectRequestContext,
  ConnectResponse,
  UnaryMethodDescriptor,
}
import org.http4s.{Header, Headers}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*
import org.typelevel.vault.Key

import scala.concurrent.duration.*

class UnaryInterceptorTest extends AnyFunSuite with Matchers {

  private val descriptor = UnaryMethodDescriptor(
    serviceName = "connectrpc.TestService",
    methodName = "Call",
    requestCompanion = connectrpc.Error,
    responseCompanion = connectrpc.Error,
  )

  private val request = connectrpc.Error()

  test("identity forwards the context unchanged") {
    val interceptor = UnaryInterceptor.identity[Id]
    val context     = ConnectRequestContext(timeout = Some(1.second))

    val response = interceptor.intercept(
      descriptor,
      request,
      context,
      downstream => {
        downstream should be theSameInstanceAs context
        ConnectResponse(request)
      },
    )

    response.message shouldBe request
  }

  test("interceptor composition merges typed attributes while keeping request metadata") {
    val firstKey                  = Key.newKey[SyncIO, String].unsafeRunSync()
    val secondKey                 = Key.newKey[SyncIO, String].unsafeRunSync()
    given AttributeReader[String] = AttributeReader.fromKey(firstKey)

    val context = ConnectRequestContext(
      headers = Headers(Header.Raw(ci"x-test", "header")),
      timeout = Some(1.second),
    )
    val interceptor = UnaryInterceptor.transformContext[Id](_.withAttribute(firstKey, "first"))
      .andThen(UnaryInterceptor.transformContext[Id] { downstream =>
        downstream.attribute[String] shouldBe Some("first")
        downstream.withAttribute(secondKey, "second")
      })

    val response = interceptor.intercept(
      descriptor,
      request,
      context,
      downstream => {
        downstream.headers shouldBe context.headers
        downstream.timeout shouldBe context.timeout
        downstream.attributes.lookup(secondKey) shouldBe Some("second")
        ConnectResponse(request.withMessage(downstream.attribute[String].getOrElse("")))
      },
    )

    response.message.getMessage shouldBe "first"
    context.attribute[String] shouldBe None
    context.attributes.lookup(secondKey) shouldBe None
  }

  test("tapError observes and preserves failures") {
    val failure                                                    = RuntimeException("failed")
    var observed: Option[(UnaryMethodDescriptor[?, ?], Throwable)] = None
    val interceptor = UnaryInterceptor.tapError[IO] { (method, error) =>
      IO {
        observed = Some(method -> error)
      }
    }

    val result = interceptor
      .intercept(
        descriptor,
        request,
        ConnectRequestContext(),
        _ => IO.raiseError(failure),
      )
      .attempt
      .unsafeRunSync()

    result shouldBe Left(failure)
    observed shouldBe Some(descriptor -> failure)
  }

  test("tapError does not run for successful responses") {
    var called      = false
    val interceptor = UnaryInterceptor.tapError[IO] { (_, _) =>
      IO {
        called = true
      }
    }

    interceptor
      .intercept(
        descriptor,
        request,
        ConnectRequestContext(),
        _ => IO.pure(ConnectResponse(request)),
      )
      .unsafeRunSync()

    called shouldBe false
  }

}
