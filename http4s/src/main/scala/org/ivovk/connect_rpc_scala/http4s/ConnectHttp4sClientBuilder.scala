package org.ivovk.connect_rpc_scala.http4s

import cats.Endo
import cats.effect.Async
import org.http4s.Uri
import org.http4s.client.Client
import org.ivovk.connect_rpc_scala.http.codec.{JsonSerdesBuilder, MessageCodec, ProtoMessageCodec}
import org.ivovk.connect_rpc_scala.http4s.client.Transport
import org.ivovk.connect_rpc_scala.service.UnaryTransport

object ConnectHttp4sClientBuilder {

  private val DefaultUserAgent = "connect-rpc-scala-http4s"

  def apply[F[_]: Async](client: Client[F]): ConnectHttp4sClientBuilder[F] = {
    val jsonCodec = JsonSerdesBuilder[F]().build.codec
    new ConnectHttp4sClientBuilder(
      client = client,
      messageCodec = jsonCodec,
      jsonCodec = jsonCodec,
      userAgent = DefaultUserAgent,
    )
  }
}

final class ConnectHttp4sClientBuilder[F[_]: Async] private (
  client: Client[F],
  messageCodec: MessageCodec[F],
  jsonCodec: MessageCodec[F],
  userAgent: String,
) {

  private def copy(
    messageCodec: MessageCodec[F] = this.messageCodec,
    jsonCodec: MessageCodec[F] = this.jsonCodec,
    userAgent: String = this.userAgent,
  ): ConnectHttp4sClientBuilder[F] =
    new ConnectHttp4sClientBuilder(client, messageCodec, jsonCodec, userAgent)

  def withJsonCodecConfigurator(
    configure: Endo[JsonSerdesBuilder[F]]
  ): ConnectHttp4sClientBuilder[F] = {
    val jsonCodec = configure(JsonSerdesBuilder[F]()).build.codec
    copy(messageCodec = jsonCodec, jsonCodec = jsonCodec)
  }

  def withUserAgent(userAgent: String): ConnectHttp4sClientBuilder[F] =
    copy(userAgent = userAgent)

  def enableBinaryFormat(): ConnectHttp4sClientBuilder[F] =
    copy(messageCodec = new ProtoMessageCodec[F])

  def build(baseUri: Uri): UnaryTransport[F] =
    Transport(
      httpClient = client,
      messageCodec = messageCodec,
      jsonCodec = jsonCodec,
      baseUri = baseUri,
      userAgent = userAgent,
    )
}
