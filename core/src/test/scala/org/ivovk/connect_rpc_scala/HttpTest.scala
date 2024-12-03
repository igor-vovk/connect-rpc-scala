package org.ivovk.connect_rpc_scala

import cats.effect.*
import cats.effect.unsafe.implicits.global
import org.http4s.client.Client
import org.http4s.dsl.io.Root
import org.http4s.headers.`Content-Type`
import org.http4s.implicits.*
import org.http4s.{Method, *}
import org.ivovk.connect_rpc_scala.http.MediaTypes
import org.ivovk.connect_rpc_scala.test.TestService.TestServiceGrpc.TestService
import org.ivovk.connect_rpc_scala.test.TestService.{AddRequest, AddResponse, GetRequest, GetResponse}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.URLEncoder
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

class HttpTest extends AnyFunSuite, Matchers {

  object TestServiceImpl extends TestService {
    override def add(request: AddRequest): Future[AddResponse] =
      Future.successful(AddResponse(request.a + request.b))

    override def get(request: GetRequest): Future[GetResponse] = {
      Future.successful(GetResponse("Key is: " + request.key))
    }
  }

  // String-JSON encoder
  given [F[_]]: EntityEncoder[F, String] = EntityEncoder.stringEncoder[F]
    .withContentType(`Content-Type`(MediaTypes.`application/json`))

  test("basic") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    ConnectRouteBuilder.forService[IO](service).build
      .flatMap { routes =>
        val client = Client.fromHttpApp(routes)

        client.run(
          Request[IO](Method.POST, uri"/org.ivovk.connect_rpc_scala.test.TestService/Add")
            .withEntity(""" { "a": 1, "b": 2} """)
        )
      }
      .use { response =>
        for
          body <- response.as[String]
        yield {
          assert(body == """{"sum":3}""")
          assert(response.status == Status.Ok)
          assert(response.headers.get[`Content-Type`].map(_.mediaType).contains(MediaTypes.`application/json`))
        }
      }
      .unsafeRunSync()
  }

  test("GET request") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    ConnectRouteBuilder.forService[IO](service).build
      .flatMap { app =>
        val client = Client.fromHttpApp(app)

        val requestJson = URLEncoder.encode("""{"key":"123"}""", Charset.`UTF-8`.nioCharset)

        client.run(
          Request[IO](
            Method.GET,
            Uri(
              path = Root / "org.ivovk.connect_rpc_scala.test.TestService" / "Get",
              query = Query.fromPairs("encoding" -> "json", "message" -> requestJson)
            )
          )
        )
      }
      .use { response =>
        for
          body <- response.as[String]
        yield {
          assert(body == """{"value":"Key is: 123"}""")
          assert(response.status == Status.Ok)
          assert(response.headers.get[`Content-Type`].map(_.mediaType).contains(MediaTypes.`application/json`))
        }
      }
      .unsafeRunSync()
  }

  test("support path prefixes") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    ConnectRouteBuilder.forService[IO](service)
      .withPathPrefix(Root / "connect")
      .build
      .flatMap { app =>
        val client = Client.fromHttpApp(app)

        client.run(
          Request[IO](Method.POST, uri"/connect/org.ivovk.connect_rpc_scala.test.TestService/Add")
            .withEntity(""" { "a": 1, "b": 2} """)
        )
      }
      .use { response =>
        for
          body <- response.as[String]
        yield {
          assert(body == """{"sum":3}""")
          assert(response.status == Status.Ok)
        }
      }
      .unsafeRunSync()
  }

  test("return 404 on unknown prefix") {
    val service = TestService.bindService(TestServiceImpl, ExecutionContext.global)

    ConnectRouteBuilder.forService[IO](service)
      .withPathPrefix(Root / "connect")
      .build
      .flatMap { app =>
        val client = Client.fromHttpApp(app)

        client.run(
          Request[IO](Method.POST, uri"/api/org.ivovk.connect_rpc_scala.test.TestService/Add")
            .withEntity(""" { "a": 1, "b": 2} """)
        )
      }
      .use { response =>
        for {
          body <- response.as[String]
        } yield {
          assert(response.status == Status.NotFound)
        }
      }
      .unsafeRunSync()
  }

}
