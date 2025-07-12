package examples.connectrpc_grpc_servers

import cats.effect.Sync
import examples.v1.eliza.{ElizaServiceFs2Grpc, SayRequest, SayResponse}
import io.grpc.Metadata

class ElizaService[F[_]: Sync] extends ElizaServiceFs2Grpc[F, Metadata] {

  def say(request: SayRequest, ctx: Metadata): F[SayResponse] =
    Sync[F].delay {
      SayResponse(s"You've said: ${request.sentence}")
    }

}
