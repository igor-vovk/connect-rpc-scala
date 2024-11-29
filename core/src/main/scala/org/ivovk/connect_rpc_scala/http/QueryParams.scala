package org.ivovk.connect_rpc_scala.http

import org.http4s.dsl.impl.QueryParamDecoderMatcher
import org.http4s.{Charset, MediaType, ParseFailure, QueryParamDecoder}

import java.net.URLDecoder

object QueryParams {

  private val encodingQPDecoder = QueryParamDecoder[String].emap {
    case "json" => Right(MediaTypes.`application/json`)
    case "proto" => Right(MediaTypes.`application/proto`)
    case other => Left(ParseFailure(other, "Invalid encoding"))
  }

  object EncodingQP extends QueryParamDecoderMatcher[MediaType]("encoding")(encodingQPDecoder)

  private val messageQPDecoder = QueryParamDecoder[String]
    .map(URLDecoder.decode(_, Charset.`UTF-8`.nioCharset))

  object MessageQP extends QueryParamDecoderMatcher[String]("message")(messageQPDecoder)

}
