package org.ivovk.connect_rpc_scala.http4s

import cats.effect.std.Dispatcher
import cats.effect.{Async, Resource}
import io.grpc.Channel
import org.http4s.MediaType
import org.http4s.client.Client
import org.ivovk.connect_rpc_scala.http.MediaTypes
import org.ivovk.connect_rpc_scala.http.codec.{JsonSerDeserBuilder, MessageCodecRegistry, ProtoMessageCodec}
import org.ivovk.connect_rpc_scala.http4s.client.Http4sChannel

class Http4sClientBuilder[F[_]: Async](client: Client[F]) {

  def build(): Resource[F, Channel] =
    for dispatcher <- Dispatcher.parallel[F](await = false)
    yield {
      val jsonSerDeser = JsonSerDeserBuilder[F]().build
      val codecRegistry = MessageCodecRegistry[F](
        jsonSerDeser.codec,
        ProtoMessageCodec[F](),
      )

      new Http4sChannel(client, dispatcher, codecRegistry.byMediaType(MediaTypes.`application/json`).get)
    }

}
