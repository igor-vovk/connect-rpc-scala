package org.ivovk.connect_rpc_scala.http

import org.http4s.dsl.impl.QueryParamDecoderMatcher
import org.http4s.{MediaType, ParseFailure, QueryParamDecoder}

object QueryParams {

  private val encodingQPDecoder = QueryParamDecoder[String].emap {
    case "json"  => Right(MediaTypes.`application/json`)
    case "proto" => Right(MediaTypes.`application/proto`)
    case other   => Left(ParseFailure(other, "Invalid encoding"))
  }

  object EncodingQP extends QueryParamDecoderMatcher[MediaType]("encoding")(encodingQPDecoder)

  object MessageQP extends QueryParamDecoderMatcher[String]("message")

}
