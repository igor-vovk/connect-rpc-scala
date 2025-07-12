package examples.zio

import examples.v1.eliza.{SayRequest, ZioEliza}
import org.http4s.server.Server as HttpServer
import zio.*
import zio.Console.printLine

object Main extends ZIOAppDefault {

  // Layer for the server, providing an http4s Server
  val serverLayer: ZLayer[Any, Throwable, HttpServer] =
    Dependencies.elizaServiceImpl >+>
      Dependencies.elizaServiceGrpcServerDefinition >+>
      Dependencies.httpServerApp >+>
      Dependencies.connectRpcServer

  // Layer for the client, providing an ElizaServiceClient
  val clientLayer: ZLayer[Any, Throwable, ZioEliza.ElizaServiceClient] =
    Dependencies.httpClient >+>
      Dependencies.connectRpcChannel >+>
      Dependencies.elizaClient

  // The effect that runs the server forever.
  // ZIO.scoped ensures the server is properly acquired and released.
  private val serverEffect =
    (ZIO.service[HttpServer] *> ZIO.never).provide(serverLayer)

  // The effect that runs the client logic
  private val clientEffect = (for {
    client <- ZIO.service[ZioEliza.ElizaServiceClient]
    _      <- printLine("Calling Eliza service...")
    resp   <- client.say(SayRequest("Hello, Eliza!"))
    _      <- printLine(s"Received response: '${resp.sentence}'")
  } yield ()).provide(clientLayer)

  override def run: ZIO[Any, Throwable, Unit] =
    for {
      _           <- printLine("Starting server...")
      serverFiber <- serverEffect.fork
      // A small delay to ensure the server is up before the client calls it.
      _ <- ZIO.sleep(1500.millis)
      _ <- clientEffect
      _ <- printLine("Stopping server...")
      _ <- serverFiber.interrupt
    } yield ()

}
