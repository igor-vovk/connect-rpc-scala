package examples.zio

import examples.v1.eliza.{ElizaServiceConnect, SayRequest, SayResponse}
import zio.{Task, ZIO}

final class ElizaService extends ElizaServiceConnect.SimpleService[Task] {

  override def say(request: SayRequest): Task[SayResponse] =
    ZIO.succeed {
      SayResponse(s"You've said: ${request.sentence}")
    }
}
