package org.ivovk.connect_rpc_scala.http4s

import org.http4s.Header
import org.http4s.headers.`Content-Length`
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, MessageCodec}
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import scalapb.GeneratedMessage

object ResponseExtensions {
  extension [F[_]](response: org.http4s.Response[F]) {
    def withMessage(
      message: GeneratedMessage
    )(using codec: MessageCodec[F], options: EncodeOptions): org.http4s.Response[F] = {
      val responseEntity = codec.encode(message, options)

      val headers = response.headers
        .put(responseEntity.headers.map(Header.ToRaw.keyValuesToRaw).toSeq*)
        .pipeIfDefined(responseEntity.length) { (hs, len) =>
          hs.withContentLength(`Content-Length`(len))
        }

      response.copy(
        headers = headers,
        body = responseEntity.body,
      )
    }
  }
}
