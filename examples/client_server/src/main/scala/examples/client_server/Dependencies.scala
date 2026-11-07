package examples.client_server

import cats.effect.{IO, Resource}
import com.comcast.ip4s.*
import examples.v1.eliza.ElizaServiceConnect
import me.ivovk.cedi.Allocator
import me.ivovk.cedi.syntax.*
import org.http4s.HttpApp
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.uri
import org.ivovk.connect_rpc_scala.http4s.{ConnectHttp4sClientBuilder, ConnectHttp4sRouteBuilder}

import scala.concurrent.duration.*

object Dependencies {

  def create(): Resource[IO, Dependencies] =
    Allocator.create[IO]().map(Dependencies(using _))

}

class Dependencies(using AllocatorIO) {

  lazy val elizaService: ElizaServiceConnect.Service[IO] = new ElizaService[IO]

  lazy val httpServerApp: HttpApp[IO] = allocate:
    ConnectHttp4sRouteBuilder
      .forService[IO](ElizaServiceConnect.bind(elizaService))
      .build

  lazy val connectRpcServer: Resource[IO, org.http4s.server.Server] =
    EmberServerBuilder.default[IO]
      .withHost(host"0.0.0.0")
      .withPort(port"8080")
      .withHttpApp(httpServerApp)
      .withShutdownTimeout(1.second)
      .build

  lazy val httpClient: Client[IO] = allocate:
    EmberClientBuilder.default[IO].build

  lazy val elizaClient: ElizaServiceConnect.Client[IO] =
    ElizaServiceConnect.Client(
      ConnectHttp4sClientBuilder[IO](httpClient).build(uri"http://localhost:8080/")
    )

}
