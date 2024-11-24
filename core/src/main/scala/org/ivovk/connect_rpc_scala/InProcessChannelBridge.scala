package org.ivovk.connect_rpc_scala

import cats.effect.{Resource, Sync}
import cats.implicits.*
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{Channel, ManagedChannel, Server, ServerServiceDefinition}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*

object InProcessChannelBridge {

  def create[F[_] : Sync](
    services: Seq[ServerServiceDefinition],
    waitForShutdown: Duration,
  ): Resource[F, Channel] = {
    for
      name <- Resource.eval(Sync[F].delay(InProcessServerBuilder.generateName()))
      server <- createServer(name, services, waitForShutdown)
      channel <- createStub(name, waitForShutdown)
    yield channel
  }

  private def createServer[F[_] : Sync](
    name: String,
    services: Seq[ServerServiceDefinition],
    waitForShutdown: Duration,
  ): Resource[F, Server] = {
    val acquire = Sync[F].delay {
      InProcessServerBuilder.forName(name)
        .directExecutor()
        .addServices(services.asJava)
        .build()
        .start()
    }
    val release = (s: Server) =>
      Sync[F].delay(s.shutdown().awaitTermination(waitForShutdown.toMillis, TimeUnit.MILLISECONDS)).void

    Resource.make(acquire)(release)
  }

  private def createStub[F[_] : Sync](
    name: String,
    waitForShutdown: Duration,
  ): Resource[F, ManagedChannel] = {
    val acquire = Sync[F].delay {
      InProcessChannelBuilder.forName(name)
        .directExecutor()
        .build()
    }
    val release = (c: ManagedChannel) =>
      Sync[F].delay(c.shutdown().awaitTermination(waitForShutdown.toMillis, TimeUnit.MILLISECONDS)).void

    Resource.make(acquire)(release)
  }

}
