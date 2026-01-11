package org.ivovk.connect_rpc_scala.http4s

import fs2.Stream
import org.http4s.{Header, Headers, Response, Status}
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, MessageCodec}
import scalapb.GeneratedMessage

object ResponseExtensions {

  def mkUnaryResponse[F[_]](
    status: Status,
    headers: Headers,
    message: GeneratedMessage,
  )(using codec: MessageCodec[F], options: EncodeOptions): Response[F] = {
    val responseEntity = codec.encode(Stream.emit(message), options)

    val h = headers
      .put(responseEntity.headers.map(Header.ToRaw.keyValuesToRaw).toSeq*)

    Response[F](
      status = status,
      headers = h,
      body = responseEntity.body,
    )
  }

//  def mkStreamingResponse[F[_]](
//    status: Status,
//    headers: Headers,
//    message: fs2.Stream[F, GeneratedMessage],
//  )(using codec: MessageCodec[F], options: EncodeOptions): Response[F] = {
//    val responseEntity = message
//      .flatMap { m =>
//        codec.encode(m, options).body.chunkAll
//      }
//
//    val h = headers
//      .put(responseEntity.headers.map(Header.ToRaw.keyValuesToRaw).toSeq*)
//      .pipeIfDefined(responseEntity.length) { (hs, len) =>
//        hs.withContentLength(`Content-Length`(len))
//      }
//
//    Response[F](
//      status = status,
//      headers = h,
//      body = responseEntity.body,
//    )
//  }
}
