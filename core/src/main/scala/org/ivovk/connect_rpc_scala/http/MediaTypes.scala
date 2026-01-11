package org.ivovk.connect_rpc_scala.http

import org.http4s.{MediaType, ParseFailure, ParseResult}

import scala.annotation.targetName

object MediaTypes {

  @targetName("applicationJson")
  val `application/json`: MediaType = MediaType.application.json

  @targetName("applicationProto")
  val `application/proto`: MediaType = MediaType.unsafeParse("application/proto")

  @targetName("applicationConnectJson")
  val `application/connect+json`: MediaType = MediaType.unsafeParse("application/connect+json")

  @targetName("applicationConnectProto")
  val `application/connect+proto`: MediaType = MediaType.unsafeParse("application/connect+proto")

  def parse(s: String): ParseResult[MediaType] = s match {
    case "application/json"  => Right(`application/json`)
    case "application/proto" => Right(`application/proto`)
    case other               => Left(ParseFailure(other, "Unsupported encoding"))
  }

  def parseShort(s: String): ParseResult[MediaType] = s match {
    case "json"  => Right(`application/json`)
    case "proto" => Right(`application/proto`)
    case other   => Left(ParseFailure(other, "Unsupported encoding"))
  }

}
