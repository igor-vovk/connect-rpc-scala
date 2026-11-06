package org.ivovk.connect_rpc_scala.codegen.e2e

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import codegen.e2e.calculator.CalculatorServiceConnect
import codegen.e2e.calculator.{AddRequest, AddResponse}
import org.http4s.client.Client
import org.http4s.implicits.*
import org.http4s.{Header, HttpApp, Method, Request}
import org.ivovk.connect_rpc_scala.http4s.{ConnectHttp4sClientBuilder, ConnectHttp4sRouteBuilder}
import org.ivovk.connect_rpc_scala.service.{ConnectException, ConnectRequest, ConnectResponse}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIString

import scala.concurrent.duration.*

class GeneratedServiceTest extends AnyFunSuite with Matchers {

  private object CalculatorServiceImpl extends CalculatorServiceConnect.Service[IO] {
    override def add(request: ConnectRequest[AddRequest]): IO[ConnectResponse[AddResponse]] =
      IO.pure(ConnectResponse(AddResponse(request.message.left + request.message.right)))
  }

  private object FailingCalculatorServiceImpl extends CalculatorServiceConnect.Service[IO] {
    override def add(request: ConnectRequest[AddRequest]): IO[ConnectResponse[AddResponse]] =
      IO.raiseError(
        ConnectException(
          connectrpc.Code.InvalidArgument,
          s"Cannot add ${request.message.left} and ${request.message.right}",
        )
      )
  }

  private object SlowCalculatorServiceImpl extends CalculatorServiceConnect.Service[IO] {
    override def add(request: ConnectRequest[AddRequest]): IO[ConnectResponse[AddResponse]] =
      IO.pure(request.message).flatMap(_ => IO.never)
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
    ConnectHttp4sRouteBuilder.forService(CalculatorServiceConnect.bind(CalculatorServiceImpl)).build
      .use { app =>
        val transport = ConnectHttp4sClientBuilder(Client.fromHttpApp(app)).build(uri"http://localhost")
        val client    = CalculatorServiceConnect.Client[IO](transport)

        client.add(ConnectRequest(AddRequest(11, 31))).map { response =>
          response.message shouldBe AddResponse(42)
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

        client.add(ConnectRequest(AddRequest(20, 22))).map { response =>
          response.message shouldBe AddResponse(42)
        }
      }
      .unsafeRunSync()
  }

  test("generated google.api.http rule serves a transcoded JSON endpoint") {
    ConnectHttp4sRouteBuilder.forService(CalculatorServiceConnect.bind(CalculatorServiceImpl)).build
      .use { app =>
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
          CalculatorServiceConnect.Client[IO](transport).add(ConnectRequest(AddRequest(20, 22)))
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
            ConnectRequest(AddRequest(20, 22), timeout = Some(10.millis))
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
        .add(ConnectRequest(AddRequest(20, 22), timeout = Some(10.millis)))
        .unsafeRunSync()
    }

    error.code shouldBe connectrpc.Code.DeadlineExceeded
  }
}
