package examples.client_server

import cats.effect.Sync
import examples.v1.eliza.{ElizaServiceConnect, SayRequest, SayResponse}

class ElizaService[F[_]: Sync] extends ElizaServiceConnect.SimpleService[F] {

  override def say(request: SayRequest): F[SayResponse] =
    Sync[F].delay {
      SayResponse(s"You've said: ${request.sentence}")
    }

}
