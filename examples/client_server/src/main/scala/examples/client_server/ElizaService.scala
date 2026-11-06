package examples.client_server

import cats.effect.Sync
import examples.v1.eliza.{ElizaServiceConnect, SayRequest, SayResponse}
import org.ivovk.connect_rpc_scala.service.{ConnectRequestContext, ConnectResponse}

class ElizaService[F[_]: Sync] extends ElizaServiceConnect.Service[F] {

  override def say(
    request: SayRequest,
    _ctx: ConnectRequestContext,
  ): F[ConnectResponse[SayResponse]] =
    Sync[F].delay {
      ConnectResponse(SayResponse(s"You've said: ${request.sentence}"))
    }

}
