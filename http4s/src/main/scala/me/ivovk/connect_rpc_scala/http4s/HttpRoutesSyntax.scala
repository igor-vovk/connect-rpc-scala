package me.ivovk.connect_rpc_scala.http4s

import cats.Monad
import org.http4s.headers.Connection
import org.http4s.{HttpApp, HttpRoutes, HttpVersion, Response}

object HttpRoutesSyntax {
  extension [F[_]](response: Response[F]) {
    /* A rejected request may have unread body bytes. Tell HTTP/1 clients that the connection cannot be reused. */
    def closeIfHttp1(version: HttpVersion): Response[F] =
      if (version.major == 1) response.putHeaders(Connection.close) else response
  }

  extension [F[_]: Monad](routes: HttpRoutes[F]) {

    /** Returns a fallback 404 and signals HTTP/1 connection closure if the unmatched request has a body. */
    def orNotFoundClosingOnUnreadBody: HttpApp[F] =
      HttpApp[F] { request =>
        routes(request).getOrElse {
          val notFound = Response.notFound[F]
          if (request.isChunked || request.contentLength.exists(_ > 0L))
            notFound.closeIfHttp1(request.httpVersion)
          else notFound
        }
      }
  }
}
