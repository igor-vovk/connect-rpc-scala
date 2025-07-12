package examples.zio

import examples.v1.eliza.{SayRequest, SayResponse, ZioEliza}
import io.grpc.StatusException
import zio.{IO, ZIO}

class ElizaService extends ZioEliza.ElizaService {

  override def say(request: SayRequest): IO[StatusException, SayResponse] =
    ZIO.succeed {
      SayResponse(s"You've said: ${request.sentence}")
    }
}
