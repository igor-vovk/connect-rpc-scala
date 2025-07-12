package examples.client_server

import cats.effect.{IO, IOApp}
import examples.v1.eliza.SayRequest
import io.grpc.Metadata
import org.typelevel.log4cats.slf4j.*

object Main extends IOApp.Simple {

  private val logger = Slf4jFactory.create[IO].getLogger

  def run: IO[Unit] =
    Dependencies.create().use { deps =>
      for {
        // Start the server
        serverFiber <- deps.connectRpcServer.useForever.start

        // Call the Eliza service
        clientResponse <- deps.elizaClient.say(SayRequest("Hello, Eliza!"), new Metadata())
        _              <- logger.info(s"Received response: ${clientResponse.sentence}")

        // Stop the server
        _ <- serverFiber.cancel
        _ <- logger.info("Server stopped")
      } yield ()
    }

}
