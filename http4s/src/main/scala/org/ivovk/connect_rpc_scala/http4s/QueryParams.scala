package org.ivovk.connect_rpc_scala.http4s

import org.http4s.dsl.impl.QueryParamDecoderMatcher
import org.http4s.{MediaType, QueryParamDecoder}
import org.ivovk.connect_rpc_scala.http.MediaTypes

object QueryParams {

  private val encodingQPDecoder: QueryParamDecoder[MediaType] =
    QueryParamDecoder[String].emap(MediaTypes.parseShort)

  object EncodingQP extends QueryParamDecoderMatcher[MediaType]("encoding")(encodingQPDecoder)

  object MessageQP extends QueryParamDecoderMatcher[String]("message")

}
