package examples.connectrpc_grpc_servers

import cats.effect.{IO, Resource}
import com.comcast.ip4s.*
import examples.v1.eliza.ElizaServiceFs2Grpc
import fs2.grpc.syntax.all.*
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import me.ivovk.cedi.Allocator
import me.ivovk.cedi.syntax.*
import org.http4s.HttpApp
import org.http4s.ember.server.EmberServerBuilder
import org.ivovk.connect_rpc_scala.http4s.ConnectHttp4sRouteBuilder

object Dependencies {

  def create(): Resource[IO, Dependencies] =
    Allocator.create[IO]().map(Dependencies(using _))

}

class Dependencies(using AllocatorIO) {

  lazy val elizaService: ElizaService[IO] = new ElizaService[IO]

  // This example uses the `fs2.grpc` syntax to allocate resources for the gRPC service, but you can also use
  // ZIO-gRPC or any other gRPC library for Scala to obtain ServerServiceDefinition of your services.
  lazy val elizaServiceGrpcDefinition: ServerServiceDefinition = allocate {
    ElizaServiceFs2Grpc.bindServiceResource[IO](elizaService)
  }

  lazy val httpApp: org.http4s.HttpApp[IO] = allocate {
    ConnectHttp4sRouteBuilder.forService[IO](elizaServiceGrpcDefinition).build
  }

  lazy val connectRpcServer: Resource[IO, org.http4s.server.Server] =
    EmberServerBuilder.default[IO]
      .withHost(host"0.0.0.0")
      .withPort(port"8080")
      .withHttpApp(httpApp)
      .build

  lazy val grpcServer: Resource[IO, io.grpc.Server] =
    NettyServerBuilder.forPort(9090)
      .addService(elizaServiceGrpcDefinition)
      .resource[IO]
      .evalMap { server =>
        IO(server.start())
      }

  lazy val run: IO[Unit] =
    (grpcServer, connectRpcServer).parTupled.useForever

}
