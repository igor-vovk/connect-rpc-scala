package examples.zio

import com.comcast.ip4s.{host, port}
import examples.v1.eliza.ZioEliza
import fs2.io.net.Network
import io.grpc.{ManagedChannel, ServerServiceDefinition, StatusException}
import org.http4s.HttpApp
import org.http4s.client.Client as HttpClient
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.uri
import org.http4s.server.Server as HttpServer
import org.ivovk.connect_rpc_scala.http4s.{ConnectHttp4sChannelBuilder, ConnectHttp4sRouteBuilder}
import scalapb.zio_grpc.ZChannel
import zio.*
import zio.interop.catz.*

import scala.concurrent.duration.*

object Dependencies {

  given Network[Task] = Network.forAsync[Task]

  // Layer for the Eliza service implementation
  val elizaServiceImpl: ULayer[ZioEliza.GElizaService[Any, StatusException]] =
    ZLayer.succeed(new ElizaService().asGeneric)

  // Layer for the gRPC server service definition
  val elizaServiceGrpcServerDefinition
    : RLayer[ZioEliza.GElizaService[Any, StatusException], ServerServiceDefinition] =
    ZLayer.fromZIO {
      ZIO.service[ZioEliza.GElizaService[Any, StatusException]].flatMap { elizaServiceImpl =>
        ZioEliza.GElizaService.genericBindable.bind(elizaServiceImpl)
      }
    }

  // Layer for the http4s app
  val httpServerApp: RLayer[ServerServiceDefinition, HttpApp[Task]] = ZLayer.scoped {
    ZIO.service[ServerServiceDefinition].flatMap { elizaServiceGrpcServerDefinition =>
      ConnectHttp4sRouteBuilder.forService[Task](elizaServiceGrpcServerDefinition).build.toScopedZIO
    }
  }

  // Layer for the http4s server
  val connectRpcServer: RLayer[HttpApp[Task], HttpServer] = ZLayer.scoped {
    ZIO.service[HttpApp[Task]].flatMap { httpApp =>
      EmberServerBuilder.default[Task]
        .withHost(host"0.0.0.0")
        .withPort(port"8080")
        .withHttpApp(httpApp)
        .withShutdownTimeout(1.second)
        .build
        .toScopedZIO
    }
  }

  // Layer for the http4s client
  val httpClient: Layer[Throwable, HttpClient[Task]] = ZLayer.scoped(
    EmberClientBuilder.default[Task].build.toScopedZIO
  )

  // Layer for the connect-rpc channel
  val connectRpcChannel: RLayer[HttpClient[Task], ManagedChannel] = ZLayer.scoped {
    ZIO.service[HttpClient[Task]].flatMap { httpClient =>
      ConnectHttp4sChannelBuilder[Task](httpClient)
        .build(uri"http://localhost:8080/")
        .map(_.toManagedChannel)
        .toScopedZIO
    }
  }

  // Layer for the Eliza client
  val elizaClient: RLayer[ManagedChannel, ZioEliza.ElizaServiceClient] = ZLayer.scoped {
    ZIO.service[ManagedChannel].flatMap { connectRpcChannel =>
      ZioEliza.ElizaServiceClient.scoped(
        ZIO.acquireRelease(
          ZIO.attempt(new ZChannel(connectRpcChannel, None, Seq.empty))
        )(_.shutdown().ignore)
      )
    }
  }
}
