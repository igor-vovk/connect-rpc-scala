package me.ivovk.connect_rpc_scala.codegen.e2e

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import codegen.e2e.calculator.{AddRequest, AddResponse, CalculatorServiceConnect}
import com.google.protobuf.ByteString
import fs2.Stream
import org.http4s.client.Client
import org.http4s.implicits.*
import org.http4s.{Header, HttpApp, Method, Request, Response, Status}
import me.ivovk.connect_rpc_scala.http.MediaTypes
import me.ivovk.connect_rpc_scala.http4s.{ConnectHttp4sClientBuilder, ConnectHttp4sRouteBuilder}
import me.ivovk.connect_rpc_scala.service.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIString
import scalapb.GeneratedMessage

import scala.concurrent.duration.*

class GeneratedServiceTest extends AnyFunSuite with Matchers {

  private object SimpleCalculatorServiceImpl extends CalculatorServiceConnect.SimpleService[IO] {
    override def add(request: AddRequest): IO[AddResponse] =
      IO.pure(AddResponse(request.left + request.right))
  }

  private object CalculatorServiceImpl extends CalculatorServiceConnect.Service[IO, ConnectRequestContext] {
    override def add(
      request: AddRequest,
      _ctx: ConnectRequestContext,
    ): IO[ConnectResponse[AddResponse]] =
      IO.pure(ConnectResponse(AddResponse(request.left + request.right)))
  }

  private object FailingCalculatorServiceImpl
      extends CalculatorServiceConnect.Service[IO, ConnectRequestContext] {
    override def add(
      request: AddRequest,
      ctx: ConnectRequestContext,
    ): IO[ConnectResponse[AddResponse]] =
      IO.raiseError(
        ConnectException(
          connectrpc.Code.InvalidArgument,
          s"Cannot add ${request.left} and ${request.right}",
        )
      )
  }

  private object SlowCalculatorServiceImpl
      extends CalculatorServiceConnect.Service[IO, ConnectRequestContext] {
    override def add(
      request: AddRequest,
      ctx: ConnectRequestContext,
    ): IO[ConnectResponse[AddResponse]] =
      IO.pure(request).flatMap(_ => IO.never)
  }

  private final case class AuthenticatedContext(connect: ConnectRequestContext, user: String)

  private final case class ServiceContext(authenticated: AuthenticatedContext, requestId: String)

  private object AuthenticationInterceptor
      extends UnaryInterceptor[IO, ConnectRequestContext, AuthenticatedContext] {

    override def intercept[
      Request <: GeneratedMessage,
      Response <: GeneratedMessage,
    ](
      _descriptor: UnaryMethodDescriptor[Request, Response],
      _request: Request,
      context: ConnectRequestContext,
      next: AuthenticatedContext => IO[ConnectResponse[Response]],
    ): IO[ConnectResponse[Response]] =
      context.headers.get("authorization").flatMap(_.headOption) match {
        case Some(user) =>
          next(AuthenticatedContext(context, user)).map { response =>
            response.copy(headers = response.headers.add("x-authenticated-user", user))
          }
        case None =>
          IO.raiseError(ConnectException(connectrpc.Code.Unauthenticated, "Missing authorization"))
      }
  }

  private object RequestIdInterceptor extends UnaryInterceptor[IO, AuthenticatedContext, ServiceContext] {

    override def intercept[
      Request <: GeneratedMessage,
      Response <: GeneratedMessage,
    ](
      descriptor: UnaryMethodDescriptor[Request, Response],
      _request: Request,
      context: AuthenticatedContext,
      next: ServiceContext => IO[ConnectResponse[Response]],
    ): IO[ConnectResponse[Response]] = {
      val requestId = context.connect.headers.get("x-request-id").flatMap(_.headOption).getOrElse("missing")
      next(ServiceContext(context, requestId)).map { response =>
        response.copy(
          headers = response.headers
            .add("x-request-id", requestId)
            .add("x-procedure", descriptor.procedure)
        )
      }
    }
  }

  private final case class ClientContext(user: String, requestId: String)

  private object ClientContextInterceptor extends UnaryInterceptor[IO, ClientContext, ConnectRequestContext] {

    override def intercept[
      Request <: GeneratedMessage,
      Response <: GeneratedMessage,
    ](
      descriptor: UnaryMethodDescriptor[Request, Response],
      _request: Request,
      context: ClientContext,
      next: ConnectRequestContext => IO[ConnectResponse[Response]],
    ): IO[ConnectResponse[Response]] =
      next(
        ConnectRequestContext(
          headers = ConnectHeaders.empty
            .add("authorization", context.user)
            .add("x-request-id", context.requestId)
        )
      ).map { response =>
        response.copy(headers = response.headers.add("x-client-procedure", descriptor.procedure))
      }
  }

  private object ContextualCalculatorServiceImpl
      extends CalculatorServiceConnect.Service[IO, ServiceContext] {
    override def add(
      request: AddRequest,
      ctx: ServiceContext,
    ): IO[ConnectResponse[AddResponse]] =
      IO {
        ctx.authenticated.user shouldBe "alice"
        ctx.requestId shouldBe "request-1"
        ConnectResponse(AddResponse(request.left + request.right))
      }
  }

  test("generates and binds a service without gRPC definitions") {
    val definition = CalculatorServiceConnect.bind(CalculatorServiceImpl)

    definition.name shouldBe "codegen.e2e.CalculatorService"
    definition.methods.map(_.descriptor.procedure) should contain(
      "/codegen.e2e.CalculatorService/Add"
    )
    CalculatorServiceConnect.METHOD_ADD.httpRule.map(_.getPost) shouldBe Some("/v1/add")
  }

  test("generated client calls generated service over http4s transport") {
    ConnectHttp4sRouteBuilder.forService(CalculatorServiceConnect.bind(CalculatorServiceImpl)).buildRoutes
      .use { routes =>
        val app       = routes.connectRoutes.orNotFound
        val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
        val client    = CalculatorServiceConnect.Client[IO](transport)

        client.add(AddRequest(11, 31), ConnectRequestContext()).map { response =>
          response.message shouldBe AddResponse(42)
        }
      }
      .unsafeRunSync()
  }

  test("typed unary interceptors transform server and client contexts") {
    val serverInterceptor = AuthenticationInterceptor.andThen(RequestIdInterceptor)

    ConnectHttp4sRouteBuilder.forService(
      CalculatorServiceConnect.bind(ContextualCalculatorServiceImpl, serverInterceptor)
    ).build
      .use { app =>
        val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
        val client    = CalculatorServiceConnect.Client[IO](transport)
          .withInterceptor(ClientContextInterceptor)
        val context = ClientContext("alice", "request-1")

        client.add(AddRequest(11, 31), context).map { response =>
          response.message shouldBe AddResponse(42)
          response.headers.get("x-authenticated-user") shouldBe Some(List("alice"))
          response.headers.get("x-request-id") shouldBe Some(List("request-1"))
          response.headers.get("x-procedure") shouldBe Some(
            List("/codegen.e2e.CalculatorService/Add")
          )
          response.headers.get("x-client-procedure") shouldBe Some(
            List("/codegen.e2e.CalculatorService/Add")
          )
        }
      }
      .unsafeRunSync()
  }

  test("generated simple client calls a simple service without protocol context") {
    ConnectHttp4sRouteBuilder.forService(
      CalculatorServiceConnect.bind(SimpleCalculatorServiceImpl)
    ).build
      .use { app =>
        val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
        val client    = CalculatorServiceConnect.SimpleClient[IO](transport)

        client.add(AddRequest(11, 31)).map(_ shouldBe AddResponse(42))
      }
      .unsafeRunSync()
  }

  test("generated client supports protobuf encoding without a gRPC runtime") {
    ConnectHttp4sRouteBuilder.forService(CalculatorServiceConnect.bind(CalculatorServiceImpl)).build
      .use { app =>
        val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app))
          .enableBinaryFormat()
          .build(uri"http://localhost")
        val client = CalculatorServiceConnect.Client[IO](transport)

        client.add(AddRequest(20, 22), ConnectRequestContext()).map { response =>
          response.message shouldBe AddResponse(42)
        }
      }
      .unsafeRunSync()
  }

  test("configuring JSON does not replace the selected protobuf encoding") {
    val service = new CalculatorServiceConnect.Service[IO, ConnectRequestContext] {
      override def add(
        request: AddRequest,
        ctx: ConnectRequestContext,
      ): IO[ConnectResponse[AddResponse]] =
        IO {
          ctx.headers.get("content-type") shouldBe Some(Seq("application/proto"))
          ConnectResponse(AddResponse(request.left + request.right))
        }
    }

    ConnectHttp4sRouteBuilder.forService(CalculatorServiceConnect.bind(service)).build.use { app =>
      val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app))
        .enableBinaryFormat()
        .withJsonCodecConfigurator(identity)
        .build(uri"http://localhost")

      CalculatorServiceConnect.Client[IO](transport)
        .add(AddRequest(20, 22), ConnectRequestContext())
        .map(_.message shouldBe AddResponse(42))
    }.unsafeRunSync()
  }

  test("server encodes errors as JSON for protobuf requests") {
    ConnectHttp4sRouteBuilder.forService(
      CalculatorServiceConnect.bind(FailingCalculatorServiceImpl)
    ).build
      .use { app =>
        val request = Request[IO](
          Method.POST,
          uri"/codegen.e2e.CalculatorService/Add",
        ).withBodyStream(Stream.emits(AddRequest(20, 22).toByteArray).covary[IO])
          .putHeaders(Header.Raw(CIString("Content-Type"), "application/proto"))

        app.run(request).flatMap { response =>
          response.bodyText.compile.string.map { body =>
            response.status shouldBe Status.BadRequest
            response.contentType.map(_.mediaType) shouldBe Some(MediaTypes.`application/json`)
            body shouldBe
              """{"code":"invalid_argument","message":"Cannot add 20 and 22"}"""
          }
        }
      }
      .unsafeRunSync()
  }

  test("binary client decodes JSON Connect errors") {
    val errorBody =
      """{"code":"invalid_argument","message":"Binary failure","details":[{"type":"example.Detail","value":"AQID"}]}"""
    val errorApp: HttpApp[IO] = Kleisli(_ =>
      IO.pure(
        Response[IO](Status.BadRequest)
          .withEntity(errorBody)
          .putHeaders(Header.Raw(CIString("Content-Type"), "application/json"))
      )
    )
    val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(errorApp))
      .enableBinaryFormat()
      .build(uri"http://localhost")

    val error = intercept[ConnectException] {
      CalculatorServiceConnect.Client[IO](transport)
        .add(AddRequest(20, 22), ConnectRequestContext())
        .unsafeRunSync()
    }

    error.code shouldBe connectrpc.Code.InvalidArgument
    error.getMessage shouldBe "Binary failure"
    error.details shouldBe Seq(
      connectrpc.ErrorDetailsAny("example.Detail", ByteString.copyFrom(Array[Byte](1, 2, 3)))
    )
  }

  test("client sends the configured user agent as a standard header") {
    val userAgent = "connect-rpc-scala-test/1.0"
    val service   = new CalculatorServiceConnect.Service[IO, ConnectRequestContext] {
      override def add(
        request: AddRequest,
        ctx: ConnectRequestContext,
      ): IO[ConnectResponse[AddResponse]] =
        IO {
          ctx.headers.get("user-agent") shouldBe Some(List(userAgent))
          ConnectResponse(AddResponse(request.left + request.right))
        }
    }

    ConnectHttp4sRouteBuilder.forService(CalculatorServiceConnect.bind(service)).build.use { app =>
      val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app))
        .withUserAgent(userAgent)
        .build(uri"http://localhost")

      CalculatorServiceConnect.Client[IO](transport)
        .add(AddRequest(20, 22), ConnectRequestContext())
        .map(_.message shouldBe AddResponse(42))
    }.unsafeRunSync()
  }

  test("generated google.api.http rule serves a transcoded JSON endpoint") {
    ConnectHttp4sRouteBuilder.forService(CalculatorServiceConnect.bind(CalculatorServiceImpl)).buildRoutes
      .use { routes =>
        val app     = routes.transcodingRoutes.orNotFound
        val request = Request[IO](Method.POST, uri"/v1/add")
          .withEntity("""{"left": 20, "right": 22}""")
          .putHeaders(Header.Raw(CIString("Content-Type"), "application/json"))

        app.run(request).flatMap { response =>
          response.bodyText.compile.string.map { body =>
            response.status.code shouldBe 200
            body shouldBe """{"result":42}"""
          }
        }
      }
      .unsafeRunSync()
  }

  test("generated client decodes Connect errors without a gRPC runtime") {
    val error = intercept[ConnectException] {
      ConnectHttp4sRouteBuilder.forService(
        CalculatorServiceConnect.bind(FailingCalculatorServiceImpl)
      ).build
        .use { app =>
          val transport =
            ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
          CalculatorServiceConnect.Client[IO](transport).add(AddRequest(20, 22), ConnectRequestContext())
        }
        .unsafeRunSync()
    }

    error.code shouldBe connectrpc.Code.InvalidArgument
    error.getMessage shouldBe "Cannot add 20 and 22"
  }

  test("server enforces the Connect request timeout") {
    val error = intercept[ConnectException] {
      ConnectHttp4sRouteBuilder.forService(
        CalculatorServiceConnect.bind(SlowCalculatorServiceImpl)
      ).build
        .use { app =>
          val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
          CalculatorServiceConnect.Client[IO](transport).add(
            AddRequest(20, 22),
            ConnectRequestContext(timeout = Some(10.millis)),
          )
        }
        .unsafeRunSync()
    }

    error.code shouldBe connectrpc.Code.DeadlineExceeded
  }

  test("client maps a local request timeout to deadline exceeded") {
    val neverRespondingApp: HttpApp[IO] = Kleisli(_ => IO.never)
    val transport                       =
      ConnectHttp4sClientBuilder(Client.fromHttpApp(neverRespondingApp)).build(uri"http://localhost")

    val error = intercept[ConnectException] {
      CalculatorServiceConnect.Client[IO](transport)
        .add(AddRequest(20, 22), ConnectRequestContext(timeout = Some(10.millis)))
        .unsafeRunSync()
    }

    error.code shouldBe connectrpc.Code.DeadlineExceeded
  }
}
