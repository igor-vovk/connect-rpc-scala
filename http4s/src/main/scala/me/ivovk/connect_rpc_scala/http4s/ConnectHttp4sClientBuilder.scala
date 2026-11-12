package me.ivovk.connect_rpc_scala.http4s

import cats.Endo
import cats.effect.Async
import org.http4s.Uri
import org.http4s.client.Client
import me.ivovk.connect_rpc_scala.http.codec.{JsonSerdesBuilder, ProtoMessageCodec}
import me.ivovk.connect_rpc_scala.http4s.client.{ClientCodecs, MessageEncoding, Transport}
import me.ivovk.connect_rpc_scala.service.UnaryTransport

object ConnectHttp4sClientBuilder {

  private val DefaultUserAgent = "connect-rpc-scala-http4s"

  def apply[F[_]: Async](client: Client[F]): ConnectHttp4sClientBuilder[F] = {
    val jsonCodec = JsonSerdesBuilder[F]().build.codec
    new ConnectHttp4sClientBuilder(
      client = client,
      codecs = ClientCodecs(
        json = jsonCodec,
        protobuf = new ProtoMessageCodec[F],
        encoding = MessageEncoding.Json,
      ),
      userAgent = DefaultUserAgent,
    )
  }
}

final class ConnectHttp4sClientBuilder[F[_]: Async] private (
  client: Client[F],
  codecs: ClientCodecs[F],
  userAgent: String,
) {

  private def copy(
    codecs: ClientCodecs[F] = this.codecs,
    userAgent: String = this.userAgent,
  ): ConnectHttp4sClientBuilder[F] =
    new ConnectHttp4sClientBuilder(client, codecs, userAgent)

  def withJsonCodecConfigurator(
    configure: Endo[JsonSerdesBuilder[F]]
  ): ConnectHttp4sClientBuilder[F] = {
    val jsonCodec = configure(JsonSerdesBuilder[F]()).build.codec
    copy(codecs = codecs.copy(json = jsonCodec))
  }

  def withUserAgent(userAgent: String): ConnectHttp4sClientBuilder[F] =
    copy(userAgent = userAgent)

  def enableBinaryFormat(): ConnectHttp4sClientBuilder[F] =
    copy(codecs = codecs.copy(encoding = MessageEncoding.Protobuf))

  def build(baseUri: Uri): UnaryTransport[F] =
    Transport(
      httpClient = client,
      codecs = codecs,
      baseUri = baseUri,
      userAgent = userAgent,
    )
}
