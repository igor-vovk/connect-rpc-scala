package org.ivovk.connect_rpc_scala.http4s.server

import cats.Monad
import cats.implicits.*
import org.http4s.HttpRoutes

final case class Routes[F[_]: Monad](
  connectRoutes: HttpRoutes[F],
  transcodingRoutes: HttpRoutes[F],
) {
  def routes: HttpRoutes[F] = connectRoutes <+> transcodingRoutes
}
