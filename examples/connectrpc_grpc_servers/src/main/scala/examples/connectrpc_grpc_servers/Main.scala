package examples.connectrpc_grpc_servers

import cats.effect.{IO, IOApp}

object Main extends IOApp.Simple {

  def run: IO[Unit] =
    Dependencies.create().use(_.run)

}
