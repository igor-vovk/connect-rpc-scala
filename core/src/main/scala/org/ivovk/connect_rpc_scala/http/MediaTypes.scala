package org.ivovk.connect_rpc_scala.http

import org.http4s.MediaType

import scala.annotation.targetName

object MediaTypes {

  @targetName("applicationJson")
  val `application/json`: MediaType = MediaType.application.json

  @targetName("applicationProto")
  val `application/proto`: MediaType = MediaType.unsafeParse("application/proto")

  val allSupported: Seq[MediaType] = List(`application/json`, `application/proto`)

}
