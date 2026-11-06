package examples.client_server

import cats.effect.{IO, Resource}
import com.comcast.ip4s.*
import examples.v1.eliza.ElizaServiceConnect
import org.http4s.HttpApp
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.uri
import org.ivovk.connect_rpc_scala.http4s.{ConnectHttp4sClientBuilder, ConnectHttp4sRouteBuilder}

import scala.concurrent.duration.*

object Dependencies {

  def create(): Resource[IO, Dependencies] =
    for
      httpApp <- ConnectHttp4sRouteBuilder
        .forService(ElizaServiceConnect.bind(ElizaService[IO]()))
        .build
      client <- EmberClientBuilder.default[IO].build
    yield Dependencies(httpApp, client)

}

class Dependencies(httpServerApp: HttpApp[IO], httpClient: Client[IO]) {

  lazy val connectRpcServer: Resource[IO, org.http4s.server.Server] =
    EmberServerBuilder.default[IO]
      .withHost(host"0.0.0.0")
      .withPort(port"8080")
      .withHttpApp(httpServerApp)
      .withShutdownTimeout(1.second)
      .build

  lazy val elizaClient: ElizaServiceConnect.Client[IO] =
    ElizaServiceConnect.Client(ConnectHttp4sClientBuilder(httpClient).build(uri"http://localhost:8080/"))

}
