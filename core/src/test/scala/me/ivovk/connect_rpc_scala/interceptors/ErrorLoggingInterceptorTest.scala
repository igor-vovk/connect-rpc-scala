package me.ivovk.connect_rpc_scala.interceptors

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import connectrpc.Code
import me.ivovk.connect_rpc_scala.{
  ConnectException,
  ConnectRequestContext,
  ConnectResponse,
  UnaryMethodDescriptor,
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.slf4j.event.Level
import org.slf4j.helpers.SubstituteLoggerFactory

import scala.jdk.CollectionConverters.*

class ErrorLoggingInterceptorTest extends AnyFunSuite with Matchers {

  private val descriptor =
    UnaryMethodDescriptor("connectrpc.TestService", "Call", connectrpc.Error, connectrpc.Error)
  private val request = connectrpc.Error()

  test("logs server errors and propagates client errors without logging them by default") {
    val events  = Ref.of[IO, Vector[(String, Throwable)]](Vector.empty).unsafeRunSync()
    val logging = ErrorLoggingInterceptor.builder[IO]
      .withLogger((message, error) => events.update(_ :+ (message -> error)))
      .build()

    val clientCodes = List(
      Code.Canceled,
      Code.InvalidArgument,
      Code.NotFound,
      Code.AlreadyExists,
      Code.PermissionDenied,
      Code.ResourceExhausted,
      Code.FailedPrecondition,
      Code.Aborted,
      Code.OutOfRange,
      Code.Unauthenticated,
    )
    val timedOut   = ConnectException(Code.DeadlineExceeded, "deadline expired")
    val unexpected = IllegalStateException("unexpected failure")

    def run(next: => IO[ConnectResponse[connectrpc.Error]]) =
      logging.intercept(descriptor, request, ConnectRequestContext(), _ => next).attempt.unsafeRunSync()

    run(IO.pure(ConnectResponse(request))) shouldBe Right(ConnectResponse(request))
    clientCodes.foreach { code =>
      val error = ConnectException(code, "request rejected")
      run(IO.raiseError(error)) shouldBe Left(error)
    }
    run(IO.raiseError(timedOut)) shouldBe Left(timedOut)
    run(throw unexpected) shouldBe Left(unexpected)

    events.get.unsafeRunSync() shouldBe Vector(
      "Connect service error: method=connectrpc.TestService/Call code=DEADLINE_EXCEEDED" -> timedOut,
      "Connect service error: method=connectrpc.TestService/Call code=INTERNAL"          -> unexpected,
    )
  }

  test("a code filter can override the default and its failures preserve the service error") {
    val events  = Ref.of[IO, Vector[Throwable]](Vector.empty).unsafeRunSync()
    val builder = ErrorLoggingInterceptor.builder[IO].withLogger((_, error) => events.update(_ :+ error))
    val error   = ConnectException(Code.Canceled, "call canceled")

    def run(filter: Code => Boolean) = {
      val logging = builder.withCodeFilter(filter).build()
      logging.intercept[connectrpc.Error, connectrpc.Error](
        descriptor,
        request,
        ConnectRequestContext(),
        _ => IO.raiseError(error),
      )
        .attempt.unsafeRunSync()
    }

    run(_ == Code.Canceled) shouldBe Left(error)
    run(_ => throw IllegalStateException("filter failed")) shouldBe Left(error)
    events.get.unsafeRunSync() shouldBe Vector(error)
  }

  test("an SLF4J logger receives the failure at ERROR level") {
    val loggers = new SubstituteLoggerFactory
    val logging = ErrorLoggingInterceptor.builder[IO].withLogger(loggers.getLogger("test")).build()
    val error   = ConnectException(Code.Unavailable, "unavailable")

    val result = logging.intercept[connectrpc.Error, connectrpc.Error](
      descriptor,
      request,
      ConnectRequestContext(),
      _ => IO.raiseError(error),
    ).attempt.unsafeRunSync()

    result shouldBe Left(error)
    loggers.getEventQueue.asScala.toList.map(event =>
      (event.getLevel, event.getMessage, event.getThrowable)
    ) shouldBe
      List(
        (
          Level.ERROR,
          "Connect service error: method=connectrpc.TestService/Call code=UNAVAILABLE",
          error,
        )
      )
  }

  test("a failed logging effect preserves the service error") {
    type Result[A] = Either[Throwable, A]

    val error   = ConnectException(Code.Unavailable, "unavailable")
    val logging = ErrorLoggingInterceptor.builder[Result]
      .withLogger((_, _) => Left(IllegalStateException("logging failed")))
      .build()

    logging.intercept[connectrpc.Error, connectrpc.Error](
      descriptor,
      request,
      ConnectRequestContext(),
      _ => Left(error),
    ) shouldBe
      Left(error)
  }
}
