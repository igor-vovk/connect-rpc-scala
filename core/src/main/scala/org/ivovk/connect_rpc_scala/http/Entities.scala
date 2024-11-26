package org.ivovk.connect_rpc_scala.http

import org.http4s.MediaType

import scala.annotation.targetName

object Entities {

  @targetName("application_proto")
  val `application/proto`: MediaType = MediaType.unsafeParse("application/proto")

}
