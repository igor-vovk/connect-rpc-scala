package org.ivovk.connect_rpc_scala.http4s

import fs2.Stream
import org.http4s.{Header, Headers, Response, Status}
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, MessageCodec}
import scalapb.GeneratedMessage

object ResponseBuilder {

  def mkUnaryResponse[F[_]](
    status: Status,
    headers: Headers,
    message: GeneratedMessage,
  )(using codec: MessageCodec[F], options: EncodeOptions): Response[F] = {
    val responseEntity = codec.encode(Stream.emit(message), options)

    Response[F](
      status = status,
      headers = headers.put(responseEntity.headers.map(Header.ToRaw.keyValuesToRaw).toSeq*),
      body = responseEntity.body,
    )
  }

  def mkStreamingResponse[F[_]](
    headers: Headers,
    messages: Stream[F, GeneratedMessage],
  )(using codec: MessageCodec[F], options: EncodeOptions): Response[F] = {
    val responseEntity = codec.encode(messages, options)

    Response[F](
      headers = headers.put(responseEntity.headers.map(Header.ToRaw.keyValuesToRaw).toSeq*),
      body = responseEntity.body,
    )
  }
}
