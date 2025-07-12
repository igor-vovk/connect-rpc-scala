package org.ivovk.connect_rpc_scala.http4s

import cats.Endo
import cats.effect.std.Dispatcher
import cats.effect.{Async, Resource}
import io.grpc.Channel
import org.http4s.Uri
import org.http4s.client.Client
import org.ivovk.connect_rpc_scala.http.codec.{JsonSerDeser, JsonSerDeserBuilder, ProtoMessageCodec}
import org.ivovk.connect_rpc_scala.http4s.client.ConnectHttp4sChannel

object ConnectHttp4sChannelBuilder {

  def apply[F[_]: Async](client: Client[F]): ConnectHttp4sChannelBuilder[F] =
    new ConnectHttp4sChannelBuilder(
      client = client,
      customJsonSerDeser = None,
      useBinaryFormat = false,
    )
}

class ConnectHttp4sChannelBuilder[F[_]: Async] private (
  client: Client[F],
  customJsonSerDeser: Option[JsonSerDeser[F]],
  useBinaryFormat: Boolean,
) {

  private def copy(
    customJsonSerDeser: Option[JsonSerDeser[F]] = customJsonSerDeser,
    useBinaryFormat: Boolean = useBinaryFormat,
  ): ConnectHttp4sChannelBuilder[F] =
    new ConnectHttp4sChannelBuilder(
      client,
      customJsonSerDeser,
      useBinaryFormat,
    )

  def withJsonCodecConfigurator(method: Endo[JsonSerDeserBuilder[F]]): ConnectHttp4sChannelBuilder[F] =
    copy(customJsonSerDeser = Some(method(JsonSerDeserBuilder[F]()).build))

  /**
   * Use protobuf binary format for messages.
   *
   * By default, JSON format is used. It is marked as experimental, so use it with caution. Only JSON format
   * is guaranteed to be stable for now.
   */
  def enableBinaryFormat(): ConnectHttp4sChannelBuilder[F] =
    copy(useBinaryFormat = true)

  def build(baseUri: Uri): Resource[F, Channel] =
    for dispatcher <- Dispatcher.parallel[F](await = false)
    yield {
      val codec =
        if useBinaryFormat then ProtoMessageCodec[F]()
        else customJsonSerDeser.getOrElse(JsonSerDeserBuilder[F]().build).codec

      val headerMapping = Http4sHeaderMapping(
        h => !"Connection".equalsIgnoreCase(h),
        _ => true,
        treatTrailersAsHeaders = true,
      )

      new ConnectHttp4sChannel(
        httpClient = client,
        dispatcher = dispatcher,
        messageCodec = codec,
        headerMapping = headerMapping,
        baseUri = baseUri,
      )
    }

}
