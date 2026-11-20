package me.ivovk.connect_rpc_scala.http4s

import cats.Monad
import me.ivovk.connect_rpc_scala.http4s.server.ConnectRoutes.closeIfHttp1
import org.http4s.{HttpApp, HttpRoutes, Response}

object HttpRoutesSyntax {
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
