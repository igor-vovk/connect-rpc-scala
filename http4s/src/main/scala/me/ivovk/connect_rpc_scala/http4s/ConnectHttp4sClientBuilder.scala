package me.ivovk.connect_rpc_scala.http4s

import cats.Endo
import cats.effect.Async
import org.http4s.Uri
import org.http4s.client.Client
import me.ivovk.connect_rpc_scala.http.codec.{CodecRegistry, JsonSerdesBuilder}
import me.ivovk.connect_rpc_scala.http4s.client.Transport
import me.ivovk.connect_rpc_scala.{ConnectTransport, MessageEncoding}

object ConnectHttp4sClientBuilder {

  private val DefaultUserAgent = "connect-rpc-scala-http4s"

  def apply[F[_]: Async](client: Client[F]): ConnectHttp4sClientBuilder[F] = {
    val jsonSerdes = JsonSerdesBuilder[F]().build
    new ConnectHttp4sClientBuilder(
      client = client,
      codecs = CodecRegistry.create(jsonSerdes),
      encoding = MessageEncoding.Protobuf,
      userAgent = DefaultUserAgent,
    )
  }
}

final class ConnectHttp4sClientBuilder[F[_]: Async] private (
  client: Client[F],
  codecs: CodecRegistry[F],
  encoding: MessageEncoding,
  userAgent: String,
) {

  private def copy(
    codecs: CodecRegistry[F] = this.codecs,
    encoding: MessageEncoding = this.encoding,
    userAgent: String = this.userAgent,
  ): ConnectHttp4sClientBuilder[F] =
    new ConnectHttp4sClientBuilder(client, codecs, encoding, userAgent)

  def withJsonCodecConfigurator(
    configure: Endo[JsonSerdesBuilder[F]]
  ): ConnectHttp4sClientBuilder[F] = {
    val jsonSerdes = configure(JsonSerdesBuilder[F]()).build
    copy(codecs = CodecRegistry.create(jsonSerdes))
  }

  def withUserAgent(userAgent: String): ConnectHttp4sClientBuilder[F] =
    copy(userAgent = userAgent)

  def withMessageEncoding(encoding: MessageEncoding): ConnectHttp4sClientBuilder[F] =
    copy(encoding = encoding)

  def build(baseUri: Uri): ConnectTransport[F] =
    Transport(
      httpClient = client,
      codecs = codecs,
      encoding = encoding,
      baseUri = baseUri,
      userAgent = userAgent,
    )
}
