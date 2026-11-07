package examples.zio

import com.comcast.ip4s.*
import examples.v1.eliza.ElizaServiceConnect
import fs2.io.net.Network
import org.http4s.HttpApp
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.uri
import org.http4s.server.Server
import org.ivovk.connect_rpc_scala.http4s.{ConnectHttp4sClientBuilder, ConnectHttp4sRouteBuilder}
import zio.interop.catz.*
import zio.{Scope, Task, ZIO}

import scala.concurrent.duration.*

object Dependencies {

  private given Network[Task] = Network.forAsync[Task]

  def create: ZIO[Scope, Throwable, Dependencies] =
    for {
      httpApp <- ConnectHttp4sRouteBuilder
        .forService(ElizaServiceConnect.bind(new ElizaService))
        .build
        .toScopedZIO
      httpClient <- EmberClientBuilder.default[Task].build.toScopedZIO
    } yield new Dependencies(httpApp, httpClient)
}

final class Dependencies private (httpApp: HttpApp[Task], httpClient: Client[Task]) {

  import Dependencies.given

  def connectRpcServer: ZIO[Scope, Throwable, Server] =
    EmberServerBuilder.default[Task]
      .withHost(host"0.0.0.0")
      .withPort(port"8080")
      .withHttpApp(httpApp)
      .withShutdownTimeout(1.second)
      .build
      .toScopedZIO

  val elizaClient: ElizaServiceConnect.Client[Task] =
    ElizaServiceConnect.Client(
      ConnectHttp4sClientBuilder(httpClient).build(uri"http://localhost:8080/")
    )
}
