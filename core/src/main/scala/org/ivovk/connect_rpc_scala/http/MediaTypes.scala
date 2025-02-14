package org.ivovk.connect_rpc_scala.http

import org.http4s.{MediaType, ParseFailure, ParseResult}

import scala.annotation.targetName

object MediaTypes {

  @targetName("applicationJson")
  val `application/json`: MediaType = MediaType.application.json

  @targetName("applicationProto")
  val `application/proto`: MediaType = MediaType.unsafeParse("application/proto")

  val allSupported: Seq[MediaType] = List(`application/json`, `application/proto`)

  def parse(s: String): ParseResult[MediaType] = s match {
    case "application/json"  => Right(`application/json`)
    case "application/proto" => Right(`application/proto`)
    case other               => Left(ParseFailure(other, "Unsupported encoding"))
  }

  def unsafeParse(s: String): MediaType = parse(s).fold(throw _, identity)

  def parseShort(s: String): ParseResult[MediaType] = s match {
    case "json"  => Right(`application/json`)
    case "proto" => Right(`application/proto`)
    case other   => Left(ParseFailure(other, "Unsupported encoding"))
  }

  def unsafeParseShort(s: String): MediaType = parseShort(s).fold(throw _, identity)

}
