package org.ivovk.connect_rpc_scala.http

import org.http4s.headers.`Content-Length`
import org.http4s.{EntityBody, Headers, Response}

import scala.util.chaining.*

case class ResponseEntity[F[_]](
  headers: Headers,
  body: EntityBody[F],
  length: Option[Long] = None,
) {

  def applyTo(response: Response[F]): Response[F] = {
    val headers = (response.headers ++ this.headers)
      .pipe(
        length match
          case Some(length) => _.withContentLength(`Content-Length`(length))
          case None         => identity
      )

    response.copy(
      headers = headers,
      body = body,
    )
  }

}
