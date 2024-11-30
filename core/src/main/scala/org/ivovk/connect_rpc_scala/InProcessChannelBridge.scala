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
    serverBuilderConfigurer: Endo[ServerBuilder[?]] = identity,
    channelBuilderConfigurer: Endo[ManagedChannelBuilder[?]] = identity,
    waitForShutdown: Duration,
  ): Resource[F, Channel] = {
    for
      name <- Resource.eval(Sync[F].delay(InProcessServerBuilder.generateName()))
      server <- createServer(name, services, waitForShutdown, serverBuilderConfigurer)
      channel <- createStub(name, waitForShutdown, channelBuilderConfigurer)
    yield channel
  }

  private def createServer[F[_] : Sync](
    name: String,
    services: Seq[ServerServiceDefinition],
    waitForShutdown: Duration,
    serverBuilderConfigurer: Endo[ServerBuilder[?]] = identity,
  ): Resource[F, Server] = {
    val acquire = Sync[F].delay {
      InProcessServerBuilder.forName(name)
        .directExecutor()
        .addServices(services.asJava)
        .pipe(serverBuilderConfigurer)
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
    channelBuilderConfigurer: Endo[ManagedChannelBuilder[?]] = identity,
  ): Resource[F, ManagedChannel] = {
    val acquire = Sync[F].delay {
      InProcessChannelBuilder.forName(name)
        .directExecutor()
        .pipe(channelBuilderConfigurer)
        .build()
    }
    val release = (c: ManagedChannel) =>
      Sync[F].delay(c.shutdown().awaitTermination(waitForShutdown.toMillis, TimeUnit.MILLISECONDS)).void

    Resource.make(acquire)(release)
  }

}
