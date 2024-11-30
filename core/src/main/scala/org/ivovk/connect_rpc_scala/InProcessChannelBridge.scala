package org.ivovk.connect_rpc_scala

import cats.Endo
import cats.effect.{Resource, Sync}
import cats.implicits.*
import io.grpc.*
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*
import scala.util.chaining.*

object InProcessChannelBridge {

  def create[F[_] : Sync](
    services: Seq[ServerServiceDefinition],
    serverBuilderConfigurator: Endo[ServerBuilder[?]] = identity,
    channelBuilderConfigurator: Endo[ManagedChannelBuilder[?]] = identity,
    waitForShutdown: Duration,
  ): Resource[F, Channel] = {
    for
      name <- Resource.eval(Sync[F].delay(InProcessServerBuilder.generateName()))
      server <- createServer(name, services, waitForShutdown, serverBuilderConfigurator)
      channel <- createStub(name, waitForShutdown, channelBuilderConfigurator)
    yield channel
  }

  private def createServer[F[_] : Sync](
    name: String,
    services: Seq[ServerServiceDefinition],
    waitForShutdown: Duration,
    serverBuilderConfigurator: Endo[ServerBuilder[?]] = identity,
  ): Resource[F, Server] = {
    val acquire = Sync[F].delay {
      InProcessServerBuilder.forName(name)
        .directExecutor()
        .addServices(services.asJava)
        .pipe(serverBuilderConfigurator)
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
    channelBuilderConfigurator: Endo[ManagedChannelBuilder[?]] = identity,
  ): Resource[F, ManagedChannel] = {
    val acquire = Sync[F].delay {
      InProcessChannelBuilder.forName(name)
        .directExecutor()
        .pipe(channelBuilderConfigurator)
        .build()
    }
    val release = (c: ManagedChannel) =>
      Sync[F].delay(c.shutdown().awaitTermination(waitForShutdown.toMillis, TimeUnit.MILLISECONDS)).void

    Resource.make(acquire)(release)
  }

}
