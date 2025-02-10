package org.ivovk.connect_rpc_scala.grpc

import cats.Endo
import cats.effect.{Resource, Sync}
import io.grpc.*
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.{Executor, TimeUnit}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*

object InProcessChannelBridge {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def create[F[_]: Sync](
    services: Seq[ServerServiceDefinition],
    serverConfigurator: Endo[ServerBuilder[?]],
    channelConfigurator: Endo[ManagedChannelBuilder[?]],
    executor: Executor,
    waitForShutdown: Duration,
  ): Resource[F, Channel] =
    for
      name    <- Resource.eval(Sync[F].delay(InProcessServerBuilder.generateName()))
      server  <- createServer(name, services, serverConfigurator, executor, waitForShutdown)
      channel <- createStub(name, channelConfigurator, executor, waitForShutdown)
    yield channel

  private def createServer[F[_]: Sync](
    name: String,
    services: Seq[ServerServiceDefinition],
    serverConfigurator: Endo[ServerBuilder[?]],
    executor: Executor,
    waitForShutdown: Duration,
  ): Resource[F, Server] = {
    val acquire = Sync[F].delay {
      InProcessServerBuilder.forName(name)
        .addServices(services.asJava)
        .executor(executor)
        .pipe(serverConfigurator)
        .build().start()
    }
    val release = (s: Server) =>
      Sync[F].delay {
        val res = s.shutdown().awaitTermination(waitForShutdown.toMillis, TimeUnit.MILLISECONDS)

        if (!res) {
          logger.warn(
            s"GRPC server did not shut down in $waitForShutdown, consider increasing shutdown timeout"
          )
        }
      }

    Resource.make(acquire)(release)
  }

  private def createStub[F[_]: Sync](
    name: String,
    channelConfigurator: Endo[ManagedChannelBuilder[?]],
    executor: Executor,
    waitForShutdown: Duration,
  ): Resource[F, ManagedChannel] = {
    val acquire = Sync[F].delay {
      InProcessChannelBuilder.forName(name)
        .executor(executor)
        .pipe(channelConfigurator)
        .build()
    }
    val release = (c: ManagedChannel) =>
      Sync[F].delay {
        val res = c.shutdown().awaitTermination(waitForShutdown.toMillis, TimeUnit.MILLISECONDS)

        if (!res) {
          logger.warn(
            s"GRPC channel did not shut down in $waitForShutdown, consider increasing shutdown timeout"
          )
        }
      }

    Resource.make(acquire)(release)
  }

}
