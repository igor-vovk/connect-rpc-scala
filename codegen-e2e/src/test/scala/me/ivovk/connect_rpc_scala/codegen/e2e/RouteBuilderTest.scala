package me.ivovk.connect_rpc_scala.codegen.e2e

import cats.data.OptionT
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import codegen.e2e.calculator.CalculatorServiceConnect
import codegen.e2e.calculator.TranscodingServiceConnect
import codegen.e2e.calculator.{AddRequest, AddResponse, GetRequest, GetResponse, NestedAddRequest}
import org.http4s.client.Client
import org.http4s.implicits.*
import org.http4s.{Header, HttpRoutes, Method, Request, Response, Status, Uri}
import me.ivovk.connect_rpc_scala.http4s.ConnectHttp4sRouteBuilder
import me.ivovk.connect_rpc_scala.service.{ConnectRequestContext, ConnectResponse}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*

class RouteBuilderTest extends AnyFunSuite with Matchers {

  private object CalculatorServiceImpl extends CalculatorServiceConnect.Service[IO, ConnectRequestContext] {
    override def add(
      request: AddRequest,
      _ctx: ConnectRequestContext,
    ): IO[ConnectResponse[AddResponse]] =
      IO.pure(ConnectResponse(AddResponse(request.left + request.right)))
  }

  private val service = CalculatorServiceConnect.bind(CalculatorServiceImpl)

  private object TranscodingServiceImpl extends TranscodingServiceConnect.Service[IO, ConnectRequestContext] {
    override def get(
      request: GetRequest,
      _ctx: ConnectRequestContext,
    ): IO[ConnectResponse[GetResponse]] =
      IO.pure(ConnectResponse(GetResponse(s"Key is: ${request.key}")))

    override def addNested(
      request: NestedAddRequest,
      _ctx: ConnectRequestContext,
    ): IO[ConnectResponse[AddResponse]] = {
      val input = request.getInput
      IO.pure(ConnectResponse(AddResponse(input.left + input.right)))
    }
  }

  private val transcodingService = TranscodingServiceConnect.bind(TranscodingServiceImpl)

  test("apply the path prefix to Connect and transcoding routes") {
    ConnectHttp4sRouteBuilder.forService(service)
      .withPathPrefix(uri"/api/v1".path)
      .build
      .use { app =>
        val client = Client.fromHttpApp(app)

        for {
          connect <- response(
            client,
            jsonPost(uri"/api/v1/codegen.e2e.CalculatorService/Add", 20, 22),
          )
          transcoding <- response(client, jsonPost(uri"/api/v1/v1/add", 20, 22))
        } yield {
          connect shouldBe (Status.Ok     -> """{"result":42}""")
          transcoding shouldBe (Status.Ok -> """{"result":42}""")
        }
      }
      .unsafeRunSync()
  }

  test("leave unknown routes unmatched") {
    ConnectHttp4sRouteBuilder.forService(service).buildRoutes
      .use { routes =>
        routes.routes.run(Request[IO](Method.POST, uri"/unknown.Service/UnknownMethod")).value
      }
      .map(_ shouldBe None)
      .unsafeRunSync()
  }

  test("return a Connect error for invalid JSON") {
    ConnectHttp4sRouteBuilder.forService(service).build
      .use { app =>
        response(
          Client.fromHttpApp(app),
          Request[IO](Method.POST, uri"/codegen.e2e.CalculatorService/Add")
            .withEntity("""{"invalid": json}""")
            .putHeaders(Header.Raw(ci"Content-Type", "application/json")),
        )
      }
      .map { case (status, _) => status shouldBe Status.BadRequest }
      .unsafeRunSync()
  }

  test("serve transcoded GET routes") {
    ConnectHttp4sRouteBuilder.forService(transcodingService).build
      .use { app =>
        response(
          Client.fromHttpApp(app),
          Request[IO](Method.GET, uri"/v1/get/prefix-test"),
        )
      }
      .map(_ shouldBe (Status.Ok -> """{"value":"Key is: prefix-test"}"""))
      .unsafeRunSync()
  }

  test("map a transcoded request body to a nested field") {
    ConnectHttp4sRouteBuilder.forService(transcodingService).build
      .use { app =>
        response(
          Client.fromHttpApp(app),
          jsonPost(uri"/v1/nested", 20, 22),
        )
      }
      .map(_ shouldBe (Status.Ok -> """{"result":42}"""))
      .unsafeRunSync()
  }

  test("compose Connect and transcoding routes") {
    ConnectHttp4sRouteBuilder.forService(service).buildRoutes
      .use { routes =>
        val client = Client.fromHttpApp(routes.routes.orNotFound)

        for {
          connect <- response(
            client,
            jsonPost(uri"/codegen.e2e.CalculatorService/Add", 1, 2),
          )
          transcoding <- response(client, jsonPost(uri"/v1/add", 20, 22))
        } yield {
          connect shouldBe (Status.Ok     -> """{"result":3}""")
          transcoding shouldBe (Status.Ok -> """{"result":42}""")
        }
      }
      .unsafeRunSync()
  }

  test("compose additional routes with generated routes") {
    val additionalRoutes = HttpRoutes[IO] { request =>
      if request.method == Method.POST && request.uri.path == uri"/custom/echo".path then
        OptionT.pure(Response[IO](Status.Ok).withEntity("echo response"))
      else OptionT.none
    }

    ConnectHttp4sRouteBuilder.forService(service)
      .withAdditionalRoutes(additionalRoutes)
      .build
      .use { app =>
        val client = Client.fromHttpApp(app)

        for {
          custom  <- response(client, Request[IO](Method.POST, uri"/custom/echo"))
          connect <- response(
            client,
            jsonPost(uri"/codegen.e2e.CalculatorService/Add", 10, 5),
          )
          transcoding <- response(client, jsonPost(uri"/v1/add", 7, 13))
        } yield {
          custom shouldBe (Status.Ok      -> "echo response")
          connect shouldBe (Status.Ok     -> """{"result":15}""")
          transcoding shouldBe (Status.Ok -> """{"result":20}""")
        }
      }
      .unsafeRunSync()
  }

  private def jsonPost(uri: Uri, left: Int, right: Int): Request[IO] =
    Request[IO](Method.POST, uri)
      .withEntity(s"""{"left":$left,"right":$right}""")
      .putHeaders(Header.Raw(ci"Content-Type", "application/json"))

  private def response(client: Client[IO], request: Request[IO]): IO[(Status, String)] =
    client.run(request).use { response =>
      response.bodyText.compile.string.map(response.status -> _)
    }
}
