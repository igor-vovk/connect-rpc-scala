package org.ivovk.connect_rpc_scala.http

import fs2.Stream

case class ResponseEntity[F[_]](
  headers: Map[String, String],
  body: Stream[F, Byte],
  length: Option[Long] = None,
)
