package me.ivovk.connect_rpc_scala.http.codec

import cats.effect.Async
import me.ivovk.connect_rpc_scala.MessageEncoding
import me.ivovk.connect_rpc_scala.http.MediaTypes
import org.http4s.MediaType

private[connect_rpc_scala] object CodecRegistry {
  def create[F[_]: Async](jsonSerdes: JsonSerdes[F]): CodecRegistry[F] = {
    val json     = jsonSerdes.codec
    val protobuf = new ProtoMessageCodec[F]

    new CodecRegistry(
      jsonCodec = json,
      protobuf = protobuf,
      streamingJson = new StreamingMessageCodec(json, jsonSerdes, MediaTypes.`application/connect+json`),
      streamingProtobuf =
        new StreamingMessageCodec(protobuf, jsonSerdes, MediaTypes.`application/connect+proto`),
    )
  }
}

private[connect_rpc_scala] final class CodecRegistry[F[_]] private (
  jsonCodec: JsonMessageCodec[F],
  protobuf: ProtoMessageCodec[F],
  streamingJson: StreamingMessageCodec[F],
  streamingProtobuf: StreamingMessageCodec[F],
) {
  def json: JsonMessageCodec[F] = jsonCodec

  def unary(encoding: MessageEncoding): MessageCodec[F] =
    encoding match {
      case MessageEncoding.Json     => json
      case MessageEncoding.Protobuf => protobuf
    }

  def unary(mediaType: MediaType): Option[MessageCodec[F]] =
    mediaType match {
      case MediaTypes.`application/json`  => Some(json)
      case MediaTypes.`application/proto` => Some(protobuf)
      case _                              => None
    }

  def streaming(encoding: MessageEncoding): StreamingMessageCodec[F] =
    encoding match {
      case MessageEncoding.Json     => streamingJson
      case MessageEncoding.Protobuf => streamingProtobuf
    }

  def streaming(mediaType: MediaType): Option[StreamingMessageCodec[F]] =
    mediaType match {
      case MediaTypes.`application/connect+json`  => Some(streamingJson)
      case MediaTypes.`application/connect+proto` => Some(streamingProtobuf)
      case _                                      => None
    }
}
