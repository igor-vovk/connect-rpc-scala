package org.ivovk.connect_rpc_scala.http4s.client

import org.ivovk.connect_rpc_scala.http.codec.{JsonMessageCodec, MessageCodec, ProtoMessageCodec}

private[http4s] enum MessageEncoding {
  case Json
  case Protobuf
}

private[http4s] final case class ClientCodecs[F[_]](
  json: JsonMessageCodec[F],
  protobuf: ProtoMessageCodec[F],
  encoding: MessageEncoding,
) {

  def chosen: MessageCodec[F] =
    encoding match {
      case MessageEncoding.Json     => json
      case MessageEncoding.Protobuf => protobuf
    }
}
