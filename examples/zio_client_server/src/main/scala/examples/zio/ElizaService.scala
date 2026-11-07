package examples.zio

import examples.v1.eliza.{ElizaServiceConnect, SayRequest, SayResponse}
import org.ivovk.connect_rpc_scala.service.{ConnectRequestContext, ConnectResponse}
import zio.{Task, ZIO}

final class ElizaService extends ElizaServiceConnect.Service[Task] {

  override def say(
    request: SayRequest,
    _ctx: ConnectRequestContext,
  ): Task[ConnectResponse[SayResponse]] =
    ZIO.succeed {
      ConnectResponse(SayResponse(s"You've said: ${request.sentence}"))
    }
}
