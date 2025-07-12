package examples.client_server

import cats.effect.{IO, Resource}
import com.comcast.ip4s.*
import examples.v1.eliza.ElizaServiceFs2Grpc
import io.grpc.{Channel, Metadata, ServerServiceDefinition}
import me.ivovk.cedi.Allocator
import me.ivovk.cedi.syntax.*
import org.http4s.HttpApp
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.uri
import org.ivovk.connect_rpc_scala.http4s.{ConnectHttp4sChannelBuilder, ConnectHttp4sRouteBuilder}
import scala.concurrent.duration.*

object Dependencies {

  def create(): Resource[IO, Dependencies] =
    Allocator.create[IO]().map(Dependencies(using _))

}

class Dependencies(using AllocatorIO) {

  // SERVER DEPENDENCIES -------------------------------------------

  lazy val elizaServiceImpl: ElizaServiceFs2Grpc[IO, Metadata] = new ElizaService

  // This example uses the `fs2.grpc` syntax to allocate resources for the gRPC service, but you can also use
  // ZIO-gRPC or any other gRPC library for Scala to obtain ServerServiceDefinition of your services.
  lazy val elizaServiceGrpcServerDefinition: ServerServiceDefinition = allocate {
    ElizaServiceFs2Grpc.bindServiceResource[IO](elizaServiceImpl)
  }

  lazy val httpServerApp: org.http4s.HttpApp[IO] = allocate {
    ConnectHttp4sRouteBuilder.forService[IO](elizaServiceGrpcServerDefinition).build
  }

  lazy val connectRpcServer: Resource[IO, org.http4s.server.Server] =
    EmberServerBuilder.default[IO]
      .withHost(host"0.0.0.0")
      .withPort(port"8080")
      .withHttpApp(httpServerApp)
      .withShutdownTimeout(1.second)
      .build

  // CLIENT DEPENDENCIES -------------------------------------------

  lazy val httpClient: org.http4s.client.Client[IO] = allocate {
    EmberClientBuilder.default[IO].build
  }

  // `Channel` is a gRPC term describing a connection to a server.
  // Both fs2-grpc and ZIO-gRPC accept Channels in their clients to communicate with a server.
  lazy val connectRpcChannel: io.grpc.Channel = allocate {
    ConnectHttp4sChannelBuilder[IO](httpClient).build(uri"http://localhost:8080/")
  }

  lazy val elizaClient: ElizaServiceFs2Grpc[IO, Metadata] = allocate {
    ElizaServiceFs2Grpc.stubResource[IO](connectRpcChannel)
  }

}
