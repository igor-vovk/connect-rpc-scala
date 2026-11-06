package examples.client_server

import cats.effect.Sync
import examples.v1.eliza.{ElizaServiceConnect, SayRequest, SayResponse}
import org.ivovk.connect_rpc_scala.service.{ConnectRequest, ConnectResponse}

class ElizaService[F[_]: Sync] extends ElizaServiceConnect.Service[F] {

  override def say(request: ConnectRequest[SayRequest]): F[ConnectResponse[SayResponse]] =
    Sync[F].delay {
      ConnectResponse(SayResponse(s"You've said: ${request.message.sentence}"))
    }

}
