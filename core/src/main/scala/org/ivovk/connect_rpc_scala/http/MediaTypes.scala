package org.ivovk.connect_rpc_scala.http

import org.http4s.MediaType

import scala.annotation.targetName

object MediaTypes {

  @targetName("applicationJson")
  val `application/json`: MediaType = MediaType.application.json

  @targetName("applicationProto")
  val `application/proto`: MediaType = MediaType.unsafeParse("application/proto")

  @targetName("applicationGrpcWebJson")
  val `application/grpc-web+json`: MediaType = MediaType.unsafeParse("application/grpc-web+json")

}
