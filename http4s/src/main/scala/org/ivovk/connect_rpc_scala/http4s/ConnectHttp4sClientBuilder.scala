package org.ivovk.connect_rpc_scala.http4s

import cats.Endo
import cats.effect.std.Dispatcher
import cats.effect.{Async, Resource}
import io.grpc.Channel
import org.http4s.Uri
import org.http4s.client.Client
import org.ivovk.connect_rpc_scala.http.codec.{JsonSerDeser, JsonSerDeserBuilder, ProtoMessageCodec}
import org.ivovk.connect_rpc_scala.http.{HeaderMapping, HeadersFilter}
import org.ivovk.connect_rpc_scala.http4s.client.Http4sChannel

import scala.concurrent.duration.Duration

object ConnectHttp4sClientBuilder {

  def apply[F[_]: Async](client: Client[F]): ConnectHttp4sClientBuilder[F] =
    new ConnectHttp4sClientBuilder(
      client = client,
      customJsonSerDeser = None,
      incomingHeadersFilter = HeaderMapping.DefaultIncomingHeadersFilter,
      outgoingHeadersFilter = HeaderMapping.DefaultOutgoingHeadersFilter,
      useBinaryFormat = false,
      requestTimeoutMs = None,
    )
}

class ConnectHttp4sClientBuilder[F[_]: Async] private (
  client: Client[F],
  customJsonSerDeser: Option[JsonSerDeser[F]],
  incomingHeadersFilter: HeadersFilter,
  outgoingHeadersFilter: HeadersFilter,
  useBinaryFormat: Boolean,
  requestTimeoutMs: Option[Long],
) {

  private def copy(
    customJsonSerDeser: Option[JsonSerDeser[F]] = customJsonSerDeser,
    incomingHeadersFilter: HeadersFilter = incomingHeadersFilter,
    outgoingHeadersFilter: HeadersFilter = outgoingHeadersFilter,
    useBinaryFormat: Boolean = useBinaryFormat,
    requestTimeoutMs: Option[Long] = requestTimeoutMs,
  ): ConnectHttp4sClientBuilder[F] =
    new ConnectHttp4sClientBuilder(
      client,
      customJsonSerDeser,
      incomingHeadersFilter,
      outgoingHeadersFilter,
      useBinaryFormat,
      requestTimeoutMs,
    )

  def withJsonCodecConfigurator(method: Endo[JsonSerDeserBuilder[F]]): ConnectHttp4sClientBuilder[F] =
    copy(customJsonSerDeser = Some(method(JsonSerDeserBuilder[F]()).build))

  /**
   * Filter for incoming headers.
   *
   * By default, headers with "connection" prefix are filtered out (GRPC requirement).
   */
  def withIncomingHeadersFilter(filter: String => Boolean): ConnectHttp4sClientBuilder[F] =
    copy(incomingHeadersFilter = filter)

  /**
   * Filter for outgoing headers.
   *
   * By default, headers with "grpc-" prefix are filtered out.
   */
  def withOutgoingHeadersFilter(filter: String => Boolean): ConnectHttp4sClientBuilder[F] =
    copy(outgoingHeadersFilter = filter)

  /**
   * Use protobuf binary format for messages.
   *
   * By default, JSON format is used.
   */
  def withUseBinaryFormat(useBinaryFormat: Boolean): ConnectHttp4sClientBuilder[F] =
    copy(useBinaryFormat = useBinaryFormat)

  def withRequestTimeout(timeout: Option[Duration]): ConnectHttp4sClientBuilder[F] =
    copy(requestTimeoutMs = timeout.map(_.toMillis).filter(_ > 0))

  def build(baseUri: Uri): Resource[F, Channel] =
    for dispatcher <- Dispatcher.parallel[F](await = false)
    yield {
      val codec =
        if useBinaryFormat then new ProtoMessageCodec[F]()
        else customJsonSerDeser.getOrElse(JsonSerDeserBuilder[F]().build).codec

      val headerMapping = Http4sHeaderMapping(
        incomingHeadersFilter,
        outgoingHeadersFilter,
        treatTrailersAsHeaders = true,
      )

      new Http4sChannel(
        client = client,
        dispatcher = dispatcher,
        messageCodec = codec,
        headerMapping = headerMapping,
        baseUri = baseUri,
        timeoutMs = requestTimeoutMs,
      )
    }

}
