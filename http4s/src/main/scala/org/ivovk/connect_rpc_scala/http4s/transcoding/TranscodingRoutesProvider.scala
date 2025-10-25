package org.ivovk.connect_rpc_scala.http4s.transcoding

import cats.MonadThrow
import cats.data.OptionT
import cats.implicits.*
import org.http4s.{Headers, HttpRoutes}
import org.ivovk.connect_rpc_scala.grpc.MergingBuilder.*
import org.ivovk.connect_rpc_scala.http.HeadersToMetadata
import org.ivovk.connect_rpc_scala.http.codec.{EntityToDecode, JsonSerDeser, MessageCodec}
import org.ivovk.connect_rpc_scala.http4s.Conversions.http4sPathToConnectRpcPath
import org.ivovk.connect_rpc_scala.transcoding.TranscodingUrlMatcher
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}
import org.ivovk.connect_rpc_scala.transcoding.MatchedRequest

class TranscodingRoutesProvider[F[_]: MonadThrow](
  urlMatcher: TranscodingUrlMatcher[F],
  handler: TranscodingHandler[F],
  headerMapping: HeadersToMetadata[Headers],
  serDeser: JsonSerDeser[F],
) {

  def routes: HttpRoutes[F] = HttpRoutes[F] { req =>
    OptionT
      .fromOption[F](
        urlMatcher.matchRequest(
          req.method,
          http4sPathToConnectRpcPath(req.uri.path),
          req.uri.query.pairs,
        )
      )
      .semiflatMap { case MatchedRequest(method, pathJson, queryJson, reqBodyTransform) =>
        given Companion[Message] = method.requestMessageCompanion

        given MessageCodec[F] = serDeser.codec.withDecodingJsonTransform(reqBodyTransform)

        val headers = headerMapping.toMetadata(req.headers)

        EntityToDecode(req.body, headers).as[Message]
          .flatMap { bodyMessage =>
            val pathMessage  = serDeser.parser.fromJson[Message](pathJson)
            val queryMessage = serDeser.parser.fromJson[Message](queryJson)

            handler.handleUnary(
              bodyMessage.merge(pathMessage).merge(queryMessage).build,
              headers,
              method,
            )
          }
      }
  }

}
