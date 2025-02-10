package org.ivovk.connect_rpc_scala.http

import org.http4s.headers.`Content-Length`
import org.http4s.{EntityBody, Headers, Response}
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*

case class ResponseEntity[F[_]](
  headers: Headers,
  body: EntityBody[F],
  length: Option[Long] = None,
) {

  def applyTo(response: Response[F]): Response[F] = {
    val headers = (response.headers ++ this.headers)
      .pipeIfDefined(length) { (hs, len) =>
        hs.withContentLength(`Content-Length`(len))
      }

    response.copy(
      headers = headers,
      body = body,
    )
  }

}
