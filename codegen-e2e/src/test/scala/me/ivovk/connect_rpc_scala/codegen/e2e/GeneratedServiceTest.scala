package me.ivovk.connect_rpc_scala.codegen.e2e

import cats.data.Kleisli
import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global
import codegen.e2e.calculator.{
  AddRequest,
  AddResponse,
  CalculatorServiceConnect,
  StreamingCalculatorServiceConnect,
}
import com.google.protobuf.ByteString
import fs2.{Chunk, Stream}
import org.http4s.client.Client
import org.http4s.implicits.*
import org.http4s.{Header, Headers, HttpApp, Method, Request, Response, Status}
import me.ivovk.connect_rpc_scala.http.MediaTypes
import me.ivovk.connect_rpc_scala.http4s.{ConnectHttp4sClientBuilder, ConnectHttp4sRouteBuilder}
import me.ivovk.connect_rpc_scala.interceptors.UnaryInterceptor
import me.ivovk.connect_rpc_scala.service.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*
import scalapb.GeneratedMessage

import scala.concurrent.duration.*

class GeneratedServiceTest extends AnyFunSuite with Matchers {

  private object StreamingCalculatorServiceImpl
      extends StreamingCalculatorServiceConnect.Service[IO, ConnectRequestContext] {
    override def addMany(
      request: AddRequest,
      _ctx: ConnectRequestContext,
    ): Resource[IO, ConnectStreamingResponse[IO, AddResponse]] = {
      val pull =
        ConnectStream.output1[IO, AddResponse](AddResponse(request.left)) >>
          ConnectStream.output1[IO, AddResponse](AddResponse(request.right)) >>
          ConnectStream.done[IO, AddResponse](Headers(Header.Raw(ci"x-count", "2")))

      Resource.pure(ConnectStreamingResponse(ConnectStream.fromPull(pull)))
    }
  }

  private object FailingStreamingCalculatorServiceImpl
      extends StreamingCalculatorServiceConnect.Service[IO, ConnectRequestContext] {
    override def addMany(
      request: AddRequest,
      _ctx: ConnectRequestContext,
    ): Resource[IO, ConnectStreamingResponse[IO, AddResponse]] = {
      val pull =
        ConnectStream.output1[IO, AddResponse](AddResponse(request.left)) >>
          fs2.Pull.raiseError[IO](
            ConnectException(
              connectrpc.Code.Unavailable,
              "response generation failed",
              trailers = Headers(Header.Raw(ci"x-failure", "generated")),
            )
          )

      Resource.pure(ConnectStreamingResponse(ConnectStream.fromPull(pull)))
    }
  }

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
          headers = Headers(Header.Raw(ci"x-error-metadata", "header")),
          trailers = Headers(Header.Raw(ci"X-Error-Metadata", "trailer")),
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
      context.headers.get(ci"authorization").map(_.head.value) match {
        case Some(user) =>
          next(AuthenticatedContext(context, user)).map { response =>
            response.copy(headers = response.headers.put(Header.Raw(ci"x-authenticated-user", user)))
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
      val requestId = context.connect.headers.get(ci"x-request-id").map(_.head.value).getOrElse("missing")
      next(ServiceContext(context, requestId)).map { response =>
        response.copy(
          headers = response.headers.put(
            Header.Raw(ci"x-request-id", requestId),
            Header.Raw(ci"x-method", descriptor.method),
          )
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
          headers = Headers(
            Header.Raw(ci"authorization", context.user),
            Header.Raw(ci"x-request-id", context.requestId),
          )
        )
      ).map { response =>
        response.copy(headers = response.headers.put(Header.Raw(ci"x-client-method", descriptor.method)))
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
    definition.methods.map(_.descriptor.method) should contain(
      "codegen.e2e.CalculatorService/Add"
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

  test("rejects payload headers in caller-supplied request metadata") {
    intercept[IllegalArgumentException] {
      ConnectRequestContext(headers = Headers(Header.Raw(ci"content-length", "999")))
    }
  }

  List("JSON", "Protobuf").foreach { encoding =>
    test(s"$encoding unary requests and responses use the encoded body length") {
      val metadata = Headers(
        Header.Raw(ci"content-length", "999"),
        Header.Raw(ci"transfer-encoding", "chunked"),
      )
      val service = new CalculatorServiceConnect.Service[IO, ConnectRequestContext] {
        override def add(
          request: AddRequest,
          _ctx: ConnectRequestContext,
        ): IO[ConnectResponse[AddResponse]] =
          IO.pure(ConnectResponse(AddResponse(request.left + request.right), headers = metadata))
      }

      def checkFraming(headers: Headers, body: Stream[IO, Byte]): IO[Chunk[Byte]] =
        body.compile.to(Chunk).map { bytes =>
          headers.get(ci"content-length").map(_.toList.map(_.value)) shouldBe Some(List(bytes.size.toString))
          headers.get(ci"transfer-encoding") shouldBe None
          bytes
        }

      ConnectHttp4sRouteBuilder.forService(CalculatorServiceConnect.bind(service)).build.use { app =>
        val checkedApp: HttpApp[IO] = Kleisli { request =>
          for {
            requestBody  <- checkFraming(request.headers, request.body)
            response     <- app.run(request.withBodyStream(Stream.chunk(requestBody)))
            responseBody <- checkFraming(response.headers, response.body)
          } yield response.withBodyStream(Stream.chunk(responseBody))
        }
        val builder  = ConnectHttp4sClientBuilder(Client.fromHttpApp(checkedApp))
        val selected = if encoding == "Protobuf" then builder.enableBinaryFormat() else builder
        val client   = CalculatorServiceConnect.Client(selected.build(uri"http://localhost"))
        val context  = ConnectRequestContext()

        for {
          response <- client.add(AddRequest(20, 22), context)
          zero     <- client.add(AddRequest.defaultInstance, context)
        } yield {
          response.message shouldBe AddResponse(42)
          zero.message shouldBe AddResponse.defaultInstance
          if encoding == "Protobuf" then
            zero.headers.get(ci"content-length").map(_.head.value) shouldBe Some("0")
        }
      }.unsafeRunSync()
    }
  }

  test("generated server-streaming client receives messages and terminal trailers") {
    ConnectHttp4sRouteBuilder
      .forService(StreamingCalculatorServiceConnect.bind(StreamingCalculatorServiceImpl))
      .build
      .use { app =>
        val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
        val client    = StreamingCalculatorServiceConnect.Client[IO](transport)

        client.addMany(AddRequest(11, 31), ConnectRequestContext()).use { response =>
          response.stream.events.compile.toList.map { events =>
            events shouldBe List(
              ConnectStream.Event.Message(AddResponse(11)),
              ConnectStream.Event.Message(AddResponse(31)),
              ConnectStream.Event.End(Headers(Header.Raw(ci"x-count", "2"))),
            )
          }
        }
      }
      .unsafeRunSync()
  }

  test("server-streaming failures preserve messages emitted before the error and error trailers") {
    ConnectHttp4sRouteBuilder
      .forService(StreamingCalculatorServiceConnect.bind(FailingStreamingCalculatorServiceImpl))
      .build
      .use { app =>
        val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
        val client    = StreamingCalculatorServiceConnect.Client[IO](transport)

        client.addMany(AddRequest(11, 31), ConnectRequestContext()).use { response =>
          response.stream.events.attempt.compile.toList.map { events =>
            events.head shouldBe Right(ConnectStream.Event.Message(AddResponse(11)))
            val error = events.last.swap.toOption.get.asInstanceOf[ConnectException]
            error.code shouldBe connectrpc.Code.Unavailable
            error.trailers.get(ci"x-failure").map(_.toList.map(_.value)) shouldBe Some(List("generated"))
          }
        }
      }
      .unsafeRunSync()
  }

  test("server-streaming route returns an error end envelope for an empty request stream") {
    ConnectHttp4sRouteBuilder
      .forService(StreamingCalculatorServiceConnect.bind(StreamingCalculatorServiceImpl))
      .build
      .use { app =>
        val request = Request[IO](
          method = Method.POST,
          uri = uri"/codegen.e2e.StreamingCalculatorService/AddMany",
        ).putHeaders(
          Header.Raw(ci"Content-Type", "application/connect+proto")
        )

        app.run(request).flatMap { response =>
          response.body.compile.to(Array).map { body =>
            response.status shouldBe Status.Ok
            response.contentLength shouldBe Some(body.length.toLong)
            body.length should be > 5
            body.head & 0x02 shouldBe 0x02
          }
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
          headerValues(response.headers, ci"x-authenticated-user") shouldBe List("alice")
          headerValues(response.headers, ci"x-request-id") shouldBe List("request-1")
          headerValues(response.headers, ci"x-method") shouldBe List("codegen.e2e.CalculatorService/Add")
          headerValues(response.headers, ci"x-client-method") shouldBe List(
            "codegen.e2e.CalculatorService/Add"
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

  test("typed unary interceptors apply to simple services") {
    ConnectHttp4sRouteBuilder.forService(
      CalculatorServiceConnect.bind(SimpleCalculatorServiceImpl, AuthenticationInterceptor)
    ).build
      .use { app =>
        val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
        val client    = CalculatorServiceConnect.Client[IO](transport)
        val context   = ConnectRequestContext(
          headers = Headers(Header.Raw(ci"authorization", "alice"))
        )

        client.add(AddRequest(11, 31), context).map { response =>
          response.message shouldBe AddResponse(42)
          headerValues(response.headers, ci"x-authenticated-user") shouldBe List("alice")
        }
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
          headerValues(ctx.headers, ci"content-type") shouldBe List("application/proto")
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
          .putHeaders(Header.Raw(ci"Content-Type", "application/proto"))

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
          .putHeaders(Header.Raw(ci"Content-Type", "application/json"))
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
          headerValues(ctx.headers, ci"user-agent") shouldBe List(userAgent)
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
          .putHeaders(Header.Raw(ci"Content-Type", "application/json"))

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
    headerValues(error.headers, ci"x-error-metadata") shouldBe List("header", "trailer")
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

  private def headerValues(headers: Headers, name: CIString): List[String] =
    headers.headers.collect { case header if header.name == name => header.value }
}
