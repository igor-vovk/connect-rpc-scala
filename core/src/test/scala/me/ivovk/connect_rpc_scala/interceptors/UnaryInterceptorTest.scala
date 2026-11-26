package me.ivovk.connect_rpc_scala.interceptors

import cats.Id
import cats.effect.{IO, Ref, SyncIO}
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
import scalapb.GeneratedMessage

import scala.concurrent.duration.*

class UnaryInterceptorTest extends AnyFunSuite with Matchers {

  private val descriptor = UnaryMethodDescriptor(
    serviceName = "connectrpc.TestService",
    methodName = "Call",
    requestCompanion = connectrpc.Error,
    responseCompanion = connectrpc.Error,
  )

  private val request = connectrpc.Error()

  test("identity and an empty chain forward the context unchanged") {
    val context = ConnectRequestContext(timeout = Some(1.second))

    List(UnaryInterceptor.identity[Id], UnaryInterceptor.chain[Id]()).foreach { interceptor =>
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

  test("a chain handles requests in order and unwinds responses and errors in reverse") {
    val response = ConnectResponse(request)
    val failure  = RuntimeException("handler failed")

    List(Right(response), Left(failure)).foreach { result =>
      val events = Ref.of[IO, Vector[String]](Vector.empty).unsafeRunSync()

      def trace(name: String): UnaryInterceptor[IO] = new UnaryInterceptor[IO] {
        override def intercept[Request <: GeneratedMessage, Response <: GeneratedMessage](
          descriptor: UnaryMethodDescriptor[Request, Response],
          request: Request,
          context: ConnectRequestContext,
          next: ConnectRequestContext => IO[ConnectResponse[Response]],
        ): IO[ConnectResponse[Response]] =
          (events.update(_ :+ s"enter $name") *> IO.defer(next(context)))
            .guarantee(events.update(_ :+ s"exit $name"))
      }

      val interceptor = UnaryInterceptor.chain(trace("first"), trace("second"), trace("third"))
      val actual      = interceptor.intercept(
        descriptor,
        request,
        ConnectRequestContext(),
        _ => events.update(_ :+ "handler") *> IO.fromEither(result),
      ).attempt.unsafeRunSync()

      actual shouldBe result
      events.get.unsafeRunSync() shouldBe
        Vector(
          "enter first",
          "enter second",
          "enter third",
          "handler",
          "exit third",
          "exit second",
          "exit first",
        )
    }
  }

  test("an outer tapError observes and preserves a rejection from a later interceptor") {
    val failure                                                    = RuntimeException("failed")
    var observed: Option[(UnaryMethodDescriptor[?, ?], Throwable)] = None
    val observe = UnaryInterceptor.tapError[IO] { (method, error) =>
      IO {
        observed = Some(method -> error)
      }
    }
    val reject      = UnaryInterceptor.transformContext[IO](_ => IO.raiseError(failure))
    val interceptor = UnaryInterceptor.chain(observe, reject)

    val result = interceptor
      .intercept(
        descriptor,
        request,
        ConnectRequestContext(),
        _ => fail("A rejected call reached the handler"),
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
