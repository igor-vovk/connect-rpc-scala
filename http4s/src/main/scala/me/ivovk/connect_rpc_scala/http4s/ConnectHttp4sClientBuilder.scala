package me.ivovk.connect_rpc_scala.http4s

import cats.Endo
import cats.effect.Async
import me.ivovk.connect_rpc_scala.http.codec.{CodecRegistry, JsonSerdesBuilder}
import me.ivovk.connect_rpc_scala.http4s.client.Transport
import me.ivovk.connect_rpc_scala.{ConnectTransport, MessageEncoding}
import org.http4s.{ContentCoding, Uri}
import org.http4s.client.Client

object ConnectHttp4sClientBuilder {

  enum RequestCompression(private[http4s] val contentCoding: ContentCoding) {
    case Identity extends RequestCompression(ContentCoding.identity)
    case Gzip     extends RequestCompression(ContentCoding.gzip)
    case Deflate  extends RequestCompression(ContentCoding.deflate)
  }

  private val DefaultUserAgent = "connect-rpc-scala-http4s"

  def apply[F[_]: Async](client: Client[F]): ConnectHttp4sClientBuilder[F] = {
    val jsonSerdes = JsonSerdesBuilder[F]().build
    new ConnectHttp4sClientBuilder(
      client = client,
      codecs = CodecRegistry.create(jsonSerdes),
      encoding = MessageEncoding.Protobuf,
      compression = RequestCompression.Identity,
      userAgent = DefaultUserAgent,
    )
  }
}

final class ConnectHttp4sClientBuilder[F[_]: Async] private (
  client: Client[F],
  codecs: CodecRegistry[F],
  encoding: MessageEncoding,
  compression: ConnectHttp4sClientBuilder.RequestCompression,
  userAgent: String,
) {
  import ConnectHttp4sClientBuilder.RequestCompression

  private def copy(
    codecs: CodecRegistry[F] = this.codecs,
    encoding: MessageEncoding = this.encoding,
    compression: RequestCompression = this.compression,
    userAgent: String = this.userAgent,
  ): ConnectHttp4sClientBuilder[F] =
    new ConnectHttp4sClientBuilder(
      client = client,
      codecs = codecs,
      encoding = encoding,
      compression = compression,
      userAgent = userAgent,
    )

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

  /** Selects the compression for outgoing requests. */
  def withRequestCompression(compression: RequestCompression): ConnectHttp4sClientBuilder[F] =
    copy(compression = compression)

  def build(baseUri: Uri): ConnectTransport[F] =
    Transport(
      httpClient = client,
      codecs = codecs,
      encoding = encoding,
      compression = compression.contentCoding,
      baseUri = baseUri,
      userAgent = userAgent,
    )
}
