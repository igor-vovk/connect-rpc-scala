package me.ivovk.connect_rpc_scala.http4s.client

import me.ivovk.connect_rpc_scala.http.codec.{JsonSerdes, MessageCodec, ProtoMessageCodec}

private[http4s] enum MessageEncoding {
  case Json
  case Protobuf
}

private[http4s] final case class ClientCodecs[F[_]](
  jsonSerdes: JsonSerdes[F],
  protobuf: ProtoMessageCodec[F],
  encoding: MessageEncoding,
) {

  def json = jsonSerdes.codec

  def chosen: MessageCodec[F] =
    encoding match {
      case MessageEncoding.Json     => jsonSerdes.codec
      case MessageEncoding.Protobuf => protobuf
    }
}
