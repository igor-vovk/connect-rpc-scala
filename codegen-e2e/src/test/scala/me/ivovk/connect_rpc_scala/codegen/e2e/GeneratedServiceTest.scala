package me.ivovk.connect_rpc_scala.codegen.e2e

import cats.data.Kleisli
import cats.effect.{IO, Resource, SyncIO}
import cats.effect.unsafe.implicits.global
import cats.syntax.foldable.*
import codegen.e2e.calculator.{
  AddRequest,
  AddResponse,
  CalculatorServiceConnect,
  StreamingCalculatorServiceConnect,
}
import com.google.protobuf.ByteString
import fs2.{Chunk, Stream}
import org.http4s.client.Client
import org.http4s.headers.{`Content-Type`, Connection}
import org.http4s.implicits.*
import org.http4s.{Header, Headers, HttpApp, HttpVersion, Method, Request, Response, Status}
import me.ivovk.connect_rpc_scala.http.MediaTypes
import me.ivovk.connect_rpc_scala.http4s.{ConnectHttp4sClientBuilder, ConnectHttp4sRouteBuilder}
import me.ivovk.connect_rpc_scala.http4s.ConnectHttp4sClientBuilder.RequestCompression
import me.ivovk.connect_rpc_scala.interceptors.{ServerStreamingInterceptor, UnaryInterceptor}
import me.ivovk.connect_rpc_scala.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*
import org.typelevel.vault.Key
import scalapb.GeneratedMessage

import scala.concurrent.duration.*

class GeneratedServiceTest extends AnyFunSuite with Matchers {

  private object StreamingCalculatorServiceImpl extends StreamingCalculatorServiceConnect.Service[IO] {
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

  private object FailingStreamingCalculatorServiceImpl extends StreamingCalculatorServiceConnect.Service[IO] {
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

  private object CalculatorServiceImpl extends CalculatorServiceConnect.Service[IO] {
    override def add(
      request: AddRequest,
      _ctx: ConnectRequestContext,
    ): IO[ConnectResponse[AddResponse]] =
      IO.pure(ConnectResponse(AddResponse(request.left + request.right)))
  }

  private object FailingCalculatorServiceImpl extends CalculatorServiceConnect.Service[IO] {
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

  private object SlowCalculatorServiceImpl extends CalculatorServiceConnect.Service[IO] {
    override def add(
      request: AddRequest,
      ctx: ConnectRequestContext,
    ): IO[ConnectResponse[AddResponse]] =
      IO.pure(request).flatMap(_ => IO.never)
  }

  private final case class Principal(user: String)
  private final case class RequestId(value: String)

  private val principalKey                 = Key.newKey[SyncIO, Principal].unsafeRunSync()
  private val requestIdKey                 = Key.newKey[SyncIO, RequestId].unsafeRunSync()
  private given AttributeReader[Principal] = AttributeReader.fromKey(principalKey)
  private given AttributeReader[RequestId] = AttributeReader.fromKey(requestIdKey)

  private object AuthenticationInterceptor extends UnaryInterceptor[IO] {

    override def intercept[
      Request <: GeneratedMessage,
      Response <: GeneratedMessage,
    ](
      _descriptor: UnaryMethodDescriptor[Request, Response],
      _request: Request,
      context: ConnectRequestContext,
      next: ConnectRequestContext => IO[ConnectResponse[Response]],
    ): IO[ConnectResponse[Response]] =
      context.headers.get(ci"authorization").map(_.head.value) match {
        case Some(user) =>
          next(context.withAttribute(principalKey, Principal(user))).map { response =>
            response.copy(headers = response.headers.put(Header.Raw(ci"x-authenticated-user", user)))
          }
        case None =>
          IO.raiseError(ConnectException(connectrpc.Code.Unauthenticated, "Missing authorization"))
      }
  }

  private object RequestIdInterceptor extends UnaryInterceptor[IO] {

    override def intercept[
      Request <: GeneratedMessage,
      Response <: GeneratedMessage,
    ](
      descriptor: UnaryMethodDescriptor[Request, Response],
      _request: Request,
      context: ConnectRequestContext,
      next: ConnectRequestContext => IO[ConnectResponse[Response]],
    ): IO[ConnectResponse[Response]] = {
      val requestId = context.headers.get(ci"x-request-id").map(_.head.value).getOrElse("missing")
      next(context.withAttribute(requestIdKey, RequestId(requestId))).map { response =>
        response.copy(
          headers = response.headers.put(
            Header.Raw(ci"x-request-id", requestId),
            Header.Raw(ci"x-method", descriptor.method),
          )
        )
      }
    }
  }

  private object SendRequestId extends UnaryInterceptor[IO] {

    override def intercept[
      Request <: GeneratedMessage,
      Response <: GeneratedMessage,
    ](
      descriptor: UnaryMethodDescriptor[Request, Response],
      _request: Request,
      context: ConnectRequestContext,
      next: ConnectRequestContext => IO[ConnectResponse[Response]],
    ): IO[ConnectResponse[Response]] =
      next(withRequestIdHeader(context)).map { response =>
        response.copy(headers = response.headers.put(Header.Raw(ci"x-client-method", descriptor.method)))
      }
  }

  private def withRequestIdHeader(context: ConnectRequestContext): ConnectRequestContext =
    context.attribute[RequestId].fold(context) { id =>
      context.copy(headers = context.headers.put(Header.Raw(ci"x-request-id", id.value)))
    }

  private def requirePrincipal(context: ConnectRequestContext): IO[Principal] =
    IO.fromOption(context.attribute[Principal])(
      ConnectException(connectrpc.Code.Unauthenticated, "Missing authenticated principal")
    )

  private object AuthenticatedCalculatorService extends CalculatorServiceConnect.Service[IO] {
    override def add(
      request: AddRequest,
      ctx: ConnectRequestContext,
    ): IO[ConnectResponse[AddResponse]] =
      requirePrincipal(ctx).map { principal =>
        principal.user shouldBe "alice"
        ctx.attribute[RequestId] shouldBe Some(RequestId("request-1"))
        ConnectResponse(AddResponse(request.left + request.right))
      }
  }

  test("generates and binds a service without gRPC definitions") {
    val definition = CalculatorServiceConnect.bind(CalculatorServiceImpl)

    definition.name shouldBe "codegen.e2e.CalculatorService"
    definition.unaryMethods.map(_.descriptor.method) should contain(
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

  test("advertises all response compressions independently of request compression") {
    ConnectHttp4sRouteBuilder.forServices(
      CalculatorServiceConnect.bind(CalculatorServiceImpl),
      StreamingCalculatorServiceConnect.bind(StreamingCalculatorServiceImpl),
    ).build.use { app =>
      List(
        RequestCompression.Identity -> None,
        RequestCompression.Gzip     -> Some("gzip"),
        RequestCompression.Deflate  -> Some("deflate"),
      ).traverse_ { (compression, expectedCoding) =>
        val checkedApp: HttpApp[IO] = Kleisli { request =>
          val streaming = request.contentType.exists(_.mediaType == MediaTypes.`application/connect+proto`)
          val accept    = if streaming then ci"Connect-Accept-Encoding" else ci"Accept-Encoding"
          val content   = if streaming then ci"Connect-Content-Encoding" else ci"Content-Encoding"

          IO {
            request.headers.get(accept).map(_.head.value) shouldBe Some("gzip, deflate")
            request.headers.get(content).map(_.head.value) shouldBe expectedCoding
          }.flatMap(_ => app.run(request))
        }
        val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(checkedApp))
          .withRequestCompression(compression)
          .build(uri"http://localhost")
        val unary     = CalculatorServiceConnect.Client(transport)
        val streaming = StreamingCalculatorServiceConnect.Client(transport)
        val request   = AddRequest(20, 22)
        val context   = ConnectRequestContext()

        unary.add(request, context).flatMap { _ =>
          streaming.addMany(request, context).use(_.stream.events.compile.drain)
        }
      }
    }.unsafeRunSync()
  }

  test("negotiates unary response compression independently of the request coding") {
    val scenarios = List(
      Some("br, gzip, deflate") -> Some("gzip"),
      Some("deflate, gzip")     -> Some("deflate"),
      Some("gzip;q=0, deflate") -> Some("deflate"),
      Some("br")                -> None,
      None                      -> Some("deflate"),
    )

    ConnectHttp4sRouteBuilder.forService(CalculatorServiceConnect.bind(CalculatorServiceImpl)).build.use {
      app =>
        scenarios.traverse_ { (accept, expectedCoding) =>
          val customAccept: HttpApp[IO] = Kleisli { request =>
            app.run(request.transformHeaders { headers =>
              headers.transform(_.filterNot(_.name == ci"Accept-Encoding")) ++
                Headers(accept.map(Header.Raw(ci"Accept-Encoding", _)))
            })
          }
          val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(customAccept))
            .withRequestCompression(RequestCompression.Deflate)
            .build(uri"http://localhost")

          CalculatorServiceConnect.Client(transport).add(AddRequest(20, 22), ConnectRequestContext()).map {
            response =>
              response.message shouldBe AddResponse(42)
              headerValues(response.headers, ci"Content-Encoding") shouldBe expectedCoding.toList
              headerValues(response.headers, ci"Vary") shouldBe List("Accept-Encoding, Content-Encoding")
              if expectedCoding.nonEmpty then headerValues(response.headers, ci"Content-Length") shouldBe Nil
          }
        }
    }.unsafeRunSync()
  }

  List(
    MessageEncoding.Json     -> MediaTypes.`application/json`,
    MessageEncoding.Protobuf -> MediaTypes.`application/proto`,
  ).foreach { (encoding, mediaType) =>
    test(s"$encoding unary requests and responses use the encoded body length") {
      val metadata = Headers(
        Header.Raw(ci"content-length", "999"),
        Header.Raw(ci"transfer-encoding", "chunked"),
      )
      val service = new CalculatorServiceConnect.Service[IO] {
        override def add(
          request: AddRequest,
          _ctx: ConnectRequestContext,
        ): IO[ConnectResponse[AddResponse]] =
          IO.pure(ConnectResponse(AddResponse(request.left + request.right), headers = metadata))
      }

      def checkFraming(headers: Headers, body: Stream[IO, Byte]): IO[Chunk[Byte]] =
        body.compile.to(Chunk).map { bytes =>
          headers.get[`Content-Type`].map(_.mediaType) shouldBe Some(mediaType)
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
        val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(checkedApp))
          .withMessageEncoding(encoding)
          .build(uri"http://localhost")
        val client  = CalculatorServiceConnect.Client(transport)
        val context = ConnectRequestContext(headers = Headers(Header.Raw(ci"Accept-Encoding", "identity")))

        for {
          response <- client.add(AddRequest(20, 22), context)
          zero     <- client.add(AddRequest.defaultInstance, context)
        } yield {
          response.message shouldBe AddResponse(42)
          zero.message shouldBe AddResponse.defaultInstance
          if encoding == MessageEncoding.Protobuf then
            zero.headers.get(ci"content-length").map(_.head.value) shouldBe Some("0")
        }
      }.unsafeRunSync()
    }
  }

  test("generated server-streaming client receives messages and terminal trailers") {
    val service = new StreamingCalculatorServiceConnect.Service[IO] {
      override def addMany(
        request: AddRequest,
        ctx: ConnectRequestContext,
      ): Resource[IO, ConnectStreamingResponse[IO, AddResponse]] =
        Resource.eval(IO {
          ctx.attribute[RequestId] shouldBe Some(RequestId("stream-1"))
        }).flatMap(_ => StreamingCalculatorServiceImpl.addMany(request, ctx))
    }
    val readRequestId = ServerStreamingInterceptor.transformContext[IO] { ctx =>
      IO {
        val id = ctx.headers.get(ci"x-request-id").map(_.head.value).getOrElse("missing")
        ctx.withAttribute(requestIdKey, RequestId(id))
      }
    }
    val sendRequestId =
      ServerStreamingInterceptor.transformContext[IO](ctx => IO.pure(withRequestIdHeader(ctx)))
    val context = ConnectRequestContext().withAttribute(requestIdKey, RequestId("stream-1"))

    ConnectHttp4sRouteBuilder
      .forService(StreamingCalculatorServiceConnect.bind(service, readRequestId))
      .build
      .use { app =>
        val builder = ConnectHttp4sClientBuilder(Client.fromHttpApp(app))
        val clients = List(
          builder.withMessageEncoding(MessageEncoding.Json) -> MediaTypes.`application/connect+json`,
          builder                                           -> MediaTypes.`application/connect+proto`,
        )

        clients.traverse_ { (configured, mediaType) =>
          val client = StreamingCalculatorServiceConnect.Client[IO](configured.build(uri"http://localhost"))
            .withInterceptor(sendRequestId)

          client.addMany(AddRequest(11, 31), context).use { response =>
            response.stream.events.compile.toList.map { events =>
              response.headers.get[`Content-Type`].map(_.mediaType) shouldBe Some(mediaType)
              events shouldBe List(
                ConnectStream.Event.Message(AddResponse(11)),
                ConnectStream.Event.Message(AddResponse(31)),
                ConnectStream.Event.End(Headers(Header.Raw(ci"x-count", "2"))),
              )
            }
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

  test("server-streaming route reports an unsupported content type") {
    ConnectHttp4sRouteBuilder
      .forService(StreamingCalculatorServiceConnect.bind(StreamingCalculatorServiceImpl))
      .build
      .use { app =>
        val request = Request[IO](
          method = Method.POST,
          uri = uri"/codegen.e2e.StreamingCalculatorService/AddMany",
        ).putHeaders(`Content-Type`(MediaTypes.`application/json`))

        app.run(request).flatMap { response =>
          response.bodyText.compile.string.map { body =>
            response.status shouldBe Status.UnsupportedMediaType
            body should startWith("Unsupported streaming media type application/json.")
          }
        }
      }
      .unsafeRunSync()
  }

  test("server-streaming request decode errors close HTTP/1 connections with unread body bytes") {
    val body = Stream.emits(Array[Byte](1, 0, 0, 0, 1, 0)) ++
      Stream.raiseError[IO](new IllegalStateException("remaining request body was read"))
    val request = Request[IO](Method.POST, uri"/codegen.e2e.StreamingCalculatorService/AddMany")
      .putHeaders(`Content-Type`(MediaTypes.`application/connect+proto`))
      .withBodyStream(body)

    ConnectHttp4sRouteBuilder
      .forService(StreamingCalculatorServiceConnect.bind(StreamingCalculatorServiceImpl))
      .build
      .use { app =>
        for {
          http1 <- app.run(request.withHttpVersion(HttpVersion.`HTTP/1.1`))
          http2 <- app.run(request.withHttpVersion(HttpVersion.`HTTP/2`))
          error <- http1.bodyText.compile.string
        } yield {
          http1.status shouldBe Status.Ok
          http2.status shouldBe Status.Ok
          http1.headers.get[Connection] shouldBe Some(Connection.close)
          http2.headers.get[Connection] shouldBe None
          error should include("Compressed envelope used without connect-content-encoding")
        }
      }
      .unsafeRunSync()
  }

  test("unary interceptors compose with typed attributes on the server and client") {
    val serverInterceptor = RequestIdInterceptor.andThen(AuthenticationInterceptor)

    ConnectHttp4sRouteBuilder.forService(
      CalculatorServiceConnect.bind(AuthenticatedCalculatorService, serverInterceptor)
    ).build
      .use { app =>
        val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
        val client    = CalculatorServiceConnect.Client[IO](transport)
          .withInterceptor(SendRequestId)
        val context = ConnectRequestContext(headers = Headers(Header.Raw(ci"authorization", "alice")))
          .withAttribute(requestIdKey, RequestId("request-1"))

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

  test("a client attribute cannot satisfy a server handler's required principal") {
    ConnectHttp4sRouteBuilder.forService(
      CalculatorServiceConnect.bind(AuthenticatedCalculatorService)
    ).build
      .use { app =>
        val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
        val client    = CalculatorServiceConnect.Client[IO](transport)
        val context   = ConnectRequestContext(headers = Headers(Header.Raw(ci"authorization", "alice")))
          .withAttribute(principalKey, Principal("alice"))

        client.add(AddRequest(11, 31), context).attempt.map {
          case Left(error: ConnectException) => error.code shouldBe connectrpc.Code.Unauthenticated
          case result                        => fail(s"Expected an unauthenticated error, got $result")
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

  test("unary interceptors apply to simple services") {
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

  test("generated unary client uses protobuf by default without a gRPC runtime") {
    ConnectHttp4sRouteBuilder.forService(CalculatorServiceConnect.bind(CalculatorServiceImpl)).build
      .use { app =>
        val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
        val client    = CalculatorServiceConnect.Client[IO](transport)

        client.add(AddRequest(20, 22), ConnectRequestContext()).map { response =>
          response.headers.get[`Content-Type`].map(_.mediaType) shouldBe Some(MediaTypes.`application/proto`)
          response.message shouldBe AddResponse(42)
        }
      }
      .unsafeRunSync()
  }

  test("configuring the JSON codec preserves the explicit JSON encoding") {
    val service = new CalculatorServiceConnect.Service[IO] {
      override def add(
        request: AddRequest,
        ctx: ConnectRequestContext,
      ): IO[ConnectResponse[AddResponse]] =
        IO {
          ctx.headers.get[`Content-Type`].map(_.mediaType) shouldBe Some(MediaTypes.`application/json`)
          ConnectResponse(AddResponse(request.left + request.right))
        }
    }

    ConnectHttp4sRouteBuilder.forService(CalculatorServiceConnect.bind(service)).build.use { app =>
      val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app))
        .withMessageEncoding(MessageEncoding.Json)
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
    val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(errorApp)).build(uri"http://localhost")

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
    val service   = new CalculatorServiceConnect.Service[IO] {
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

  test("generated client decodes compressed Connect errors without a gRPC runtime") {
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
    headerValues(error.headers, ci"Content-Encoding") shouldBe List("gzip")
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
