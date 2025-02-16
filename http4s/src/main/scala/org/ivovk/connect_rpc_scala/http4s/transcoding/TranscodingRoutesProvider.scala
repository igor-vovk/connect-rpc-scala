package org.ivovk.connect_rpc_scala.http4s.transcoding

import cats.MonadThrow
import cats.data.OptionT
import cats.implicits.*
import org.http4s.{Headers, HttpRoutes}
import org.ivovk.connect_rpc_scala.HeadersToMetadata
import org.ivovk.connect_rpc_scala.grpc.MergingBuilder.*
import org.ivovk.connect_rpc_scala.http.RequestEntity
import org.ivovk.connect_rpc_scala.http.codec.{JsonSerDeser, MessageCodec}
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
          req.uri.path.segments.map(_.encoded).toList,
          req.uri.query.pairs,
        )
      )
      .semiflatMap { case MatchedRequest(method, pathJson, queryJson, reqBodyTransform) =>
        given Companion[Message] = method.requestMessageCompanion

        given MessageCodec[F] = serDeser.codec.withDecodingJsonTransform(reqBodyTransform)

        val headers = headerMapping.toMetadata(req.headers)

        RequestEntity(req.body, headers).as[Message]
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
