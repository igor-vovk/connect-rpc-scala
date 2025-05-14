package org.ivovk.connect_rpc_scala.http4s

import cats.data.Kleisli
import cats.effect.*
import cats.effect.unsafe.implicits.global
import org.http4s.client.Client
import org.http4s.dsl.io.Root
import org.http4s.headers.`Content-Type`
import org.http4s.implicits.*
import org.http4s.{Method, *}
import org.ivovk.connect_rpc_scala.http.MediaTypes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import test.HttpCommunicationTest.TestServiceGrpc.TestService
import test.HttpCommunicationTest.*

import java.net.URLEncoder
import scala.concurrent.{ExecutionContext, Future}

class HttpCommunicationTest extends AnyFunSuite, Matchers {

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

  test("POST request") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    Http4sRouteBuilder.forService[IO](service).build
      .flatMap { routes =>
        Client.fromHttpApp(routes).run(
          Request[IO](Method.POST, uri"/test.TestService/Add").withEntity(""" { "a": 1, "b": 2} """)
        )
      }
      .use { response =>
        for body <- response.as[String]
        yield {
          assert(body == """{"sum":3}""")
          assert(response.status == Status.Ok)
          assert(
            response.headers.get[`Content-Type`].map(_.mediaType).contains(MediaTypes.`application/json`)
          )
        }
      }
      .unsafeRunSync()
  }

  test("GET request") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    Http4sRouteBuilder.forService[IO](service).build
      .flatMap { app =>
        val requestJson = URLEncoder.encode("""{"key":"123"}""", Charset.`UTF-8`.nioCharset)

        Client.fromHttpApp(app).run(
          Request[IO](
            Method.GET,
            Uri(
              path = Root / "test.TestService" / "Get",
              query = Query.fromPairs("encoding" -> "json", "message" -> requestJson),
            ),
          )
        )
      }
      .use { response =>
        for body <- response.as[String]
        yield {
          assert(body == """{"value":"Key is: 123"}""")
          assert(response.status == Status.Ok)
          assert(
            response.headers.get[`Content-Type`].map(_.mediaType).contains(MediaTypes.`application/json`)
          )
        }
      }
      .unsafeRunSync()
  }

  test("Http-annotated GET request") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    Http4sRouteBuilder.forService[IO](service).build
      .flatMap { app =>
        Client.fromHttpApp(app).run(
          Request[IO](Method.GET, uri"/get/123")
        )
      }
      .use { response =>
        for body <- response.as[String]
        yield {
          assert(body == """{"value":"Key is: 123"}""")
          assert(response.status == Status.Ok)
          assert(
            response.headers.get[`Content-Type`].map(_.mediaType).contains(MediaTypes.`application/json`)
          )
        }
      }
      .unsafeRunSync()
  }

  test("Http field mapping") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    Http4sRouteBuilder.forService[IO](service).build
      .flatMap { app =>
        Client.fromHttpApp(app).run(
          Request[IO](Method.POST, uri"/body_mapping").withEntity(""" { "a": 1, "b": 2} """)
        )
      }
      .use { response =>
        for body <- response.as[String]
        yield {
          assert(body == """{"requested":{"a":1,"b":2}}""")
          assert(response.status == Status.Ok)
          assert(
            response.headers.get[`Content-Type`].map(_.mediaType).contains(MediaTypes.`application/json`)
          )
        }
      }
      .unsafeRunSync()
  }

  test("support path prefixes") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    Http4sRouteBuilder.forService[IO](service)
      .withPathPrefix(Root / "connect")
      .build
      .flatMap { app =>
        Client.fromHttpApp(app).run(
          Request[IO](Method.POST, uri"/connect/test.TestService/Add").withEntity(""" { "a": 1, "b": 2} """)
        )
      }
      .use { response =>
        for body <- response.as[String]
        yield {
          assert(body == """{"sum":3}""")
          assert(response.status == Status.Ok)
        }
      }
      .unsafeRunSync()
  }

  test("return 404 on unknown prefix") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    Http4sRouteBuilder.forService[IO](service)
      .withPathPrefix(Root / "connect")
      .build
      .flatMap { app =>
        Client.fromHttpApp(app).run(
          Request[IO](Method.POST, uri"/api/test.TestService/Add").withEntity(""" { "a": 1, "b": 2} """)
        )
      }
      .use { response =>
        for {
          body <- response.as[String]
        } yield assert(response.status == Status.NotFound)
      }
      .unsafeRunSync()
  }

  test("fallback to the default handler when service/method is missing") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    val fallbackResponse = Response[IO](Status.MovedPermanently).withEntity("fallback")

    Http4sRouteBuilder.forService[IO](service)
      .buildRoutes
      .map(r => Kleisli((a: Request[IO]) => r.connect.run(a).getOrElse(fallbackResponse)))
      .flatMap { app =>
        Client.fromHttpApp(app).run(
          Request[IO](Method.POST, uri"/test.TestService/MethodNotFound")
        )
      }
      .use { response =>
        for {
          body <- response.as[String]
        } yield {
          assert(response.status == Status.MovedPermanently)
          assert(body == "fallback")
        }
      }
      .unsafeRunSync()
  }

}
