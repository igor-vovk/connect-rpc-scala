package org.ivovk.connect_rpc_scala.http4s

import cats.Endo
import cats.effect.Async
import org.http4s.Uri
import org.http4s.client.Client
import org.ivovk.connect_rpc_scala.http.codec.{JsonSerdesBuilder, MessageCodec, ProtoMessageCodec}
import org.ivovk.connect_rpc_scala.http4s.client.Transport
import org.ivovk.connect_rpc_scala.service.UnaryTransport

object ConnectHttp4sClientBuilder {

  def apply[F[_]: Async](client: Client[F]): ConnectHttp4sClientBuilder[F] = {
    val messageCodec = JsonSerdesBuilder[F]().build.codec
    new ConnectHttp4sClientBuilder(
      client = client,
      messageCodec = messageCodec,
    )
  }
}

final class ConnectHttp4sClientBuilder[F[_]: Async] private (
  client: Client[F],
  messageCodec: MessageCodec[F],
) {

  private def copy(newMessageCodec: MessageCodec[F]): ConnectHttp4sClientBuilder[F] =
    new ConnectHttp4sClientBuilder(client, newMessageCodec)

  def withJsonCodecConfigurator(
    configure: Endo[JsonSerdesBuilder[F]]
  ): ConnectHttp4sClientBuilder[F] =
    copy(configure(JsonSerdesBuilder[F]()).build.codec)

  def enableBinaryFormat(): ConnectHttp4sClientBuilder[F] =
    copy(new ProtoMessageCodec[F])

  def build(baseUri: Uri): UnaryTransport[F] =
    Transport(client, messageCodec, baseUri)
}
