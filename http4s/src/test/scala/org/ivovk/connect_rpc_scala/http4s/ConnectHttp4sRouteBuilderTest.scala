package org.ivovk.connect_rpc_scala.http4s

import cats.effect.*
import cats.effect.unsafe.implicits.global
import org.http4s.client.Client
import org.http4s.dsl.io.*
import org.http4s.headers.`Content-Type`
import org.http4s.implicits.*
import org.http4s.{Method, *}
import org.ivovk.connect_rpc_scala.http.MediaTypes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import test.HttpCommunicationTest.TestServiceGrpc.TestService
import test.HttpCommunicationTest.*

import scala.concurrent.{ExecutionContext, Future}

class ConnectHttp4sRouteBuilderTest extends AnyFunSuite with Matchers {

  object TestServiceImpl extends TestService {
    override def add(request: AddRequest): Future[AddResponse] =
      Future.successful(AddResponse(request.a + request.b))

    override def get(request: GetRequest): Future[GetResponse] =
      Future.successful(GetResponse("Key is: " + request.key))

    override def requestBodyMapping(request: RequestBodyMappingRequest): Future[RequestBodyMappingResponse] =
      Future.successful(RequestBodyMappingResponse(request.subRequest))
  }

  // String-JSON encoder
  given [F[_]]: EntityEncoder[F, String] = EntityEncoder.stringEncoder[F]
    .withContentType(`Content-Type`(MediaTypes.`application/json`))

  test("ConnectHttp4sRouteBuilder should support custom path prefixes") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    ConnectHttp4sRouteBuilder.forService[IO](service)
      .withPathPrefix(Root / "api" / "v1")
      .build
      .flatMap { app =>
        Client.fromHttpApp(app).run(
          Request[IO](Method.POST, uri"/api/v1/test.TestService/Add")
            .withEntity("""{"a": 7, "b": 13}""")
        )
      }
      .use { response =>
        for body <- response.as[String]
        yield {
          response.status shouldBe Status.Ok
          body shouldBe """{"sum":20}"""
        }
      }
      .unsafeRunSync()
  }

  test("ConnectHttp4sRouteBuilder should support custom path prefixes for transcoding") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    ConnectHttp4sRouteBuilder.forService[IO](service)
      .withPathPrefix(Root / "api" / "v2")
      .build
      .flatMap { app =>
        Client.fromHttpApp(app).run(
          Request[IO](Method.GET, uri"/api/v2/get/prefix-test")
        )
      }
      .use { response =>
        for body <- response.as[String]
        yield {
          response.status shouldBe Status.Ok
          body shouldBe """{"value":"Key is: prefix-test"}"""
        }
      }
      .unsafeRunSync()
  }

  test("ConnectHttp4sRouteBuilder should return 404 for unknown routes") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    ConnectHttp4sRouteBuilder.forService[IO](service).build
      .flatMap { app =>
        Client.fromHttpApp(app).run(
          Request[IO](Method.POST, uri"/unknown.Service/UnknownMethod")
            .withEntity("""{"data": "test"}""")
        )
      }
      .use { response =>
        IO {
          response.status shouldBe Status.NotFound
        }
      }
      .unsafeRunSync()
  }

  test("ConnectHttp4sRouteBuilder should handle invalid JSON gracefully") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    ConnectHttp4sRouteBuilder.forService[IO](service).build
      .flatMap { app =>
        Client.fromHttpApp(app).run(
          Request[IO](Method.POST, uri"/test.TestService/Add")
            .withEntity("""{"invalid": json}""")
        )
      }
      .use { response =>
        IO {
          response.status shouldBe Status.BadRequest
        }
      }
      .unsafeRunSync()
  }

  test("ConnectHttp4sRouteBuilder should combine connect and transcoding routes correctly") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    ConnectHttp4sRouteBuilder.forService[IO](service).buildRoutes
      .use { routes =>
        val client = Client.fromHttpApp(routes.all.orNotFound)

        for {
          // Test Connect protocol route
          connectResponse <- client.run(
            Request[IO](Method.POST, uri"/test.TestService/Add")
              .withEntity("""{"a": 1, "b": 2}""")
          ).use(_.as[String])

          // Test Transcoding route
          transcodingResponse <- client.run(
            Request[IO](Method.GET, uri"/get/combined-test")
          ).use(_.as[String])

        } yield {
          connectResponse shouldBe """{"sum":3}"""
          transcodingResponse shouldBe """{"value":"Key is: combined-test"}"""
        }
      }
      .unsafeRunSync()
  }

  test("ConnectHttp4sRouteBuilder should support additional custom routes") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    // Create custom additional routes
    val additionalRoutes = HttpRoutes.of[IO] { case POST -> Root / "custom" / "echo" =>
      IO(Response[IO](Status.Ok).withEntity("""{"message":"echo response"}"""))
    }

    ConnectHttp4sRouteBuilder.forService[IO](service)
      .withAdditionalRoutes(additionalRoutes)
      .build
      .use { app =>
        val client = Client.fromHttpApp(app)

        for {
          echoResponse <- client.run(
            Request[IO](Method.POST, uri"/custom/echo")
          ).use(_.as[String])

          // Test that Connect routes still work
          connectResponse <- client.run(
            Request[IO](Method.POST, uri"/test.TestService/Add")
              .withEntity("""{"a": 10, "b": 5}""")
          ).use(_.as[String])

          // Test that Transcoding routes still work
          transcodingResponse <- client.run(
            Request[IO](Method.GET, uri"/get/additional-test")
          ).use(_.as[String])

        } yield {
          echoResponse shouldBe """{"message":"echo response"}"""
          connectResponse shouldBe """{"sum":15}"""
          transcodingResponse shouldBe """{"value":"Key is: additional-test"}"""
        }
      }
      .unsafeRunSync()
  }
}
