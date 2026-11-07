package examples.zio

import examples.v1.eliza.SayRequest
import org.ivovk.connect_rpc_scala.service.ConnectRequestContext
import zio.Console.printLine
import zio.{ZIO, ZIOAppDefault}

object Main extends ZIOAppDefault {

  override val run: ZIO[Any, Throwable, Unit] =
    ZIO.scoped {
      for {
        dependencies <- Dependencies.create
        _            <- dependencies.connectRpcServer
        _            <- printLine("Calling Eliza service...")
        response     <- dependencies.elizaClient.say(SayRequest("Hello, Eliza!"), ConnectRequestContext())
        _            <- printLine(s"Received response: '${response.message.sentence}'")
      } yield ()
    }
}
