package org.ivovk.connect_rpc_scala.transcoding

import cats.MonadThrow
import cats.data.OptionT
import cats.implicits.*
import org.http4s.HttpRoutes
import org.ivovk.connect_rpc_scala.grpc.MergingBuilder.*
import org.ivovk.connect_rpc_scala.http.RequestEntity
import org.ivovk.connect_rpc_scala.http.codec.{JsonSerDeser, MessageCodec}
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

class TranscodingRoutesProvider[F[_] : MonadThrow](
  urlMatcher: TranscodingUrlMatcher[F],
  handler: TranscodingHandler[F],
  serDeser: JsonSerDeser[F]
) {
  def routes: HttpRoutes[F] = HttpRoutes[F] { req =>
    OptionT.fromOption[F](urlMatcher.matchRequest(req))
      .semiflatMap { case MatchedRequest(method, pathJson, queryJson, reqBodyTransform) =>
        given Companion[Message] = method.requestMessageCompanion

        given MessageCodec[F] = serDeser.codec.withDecodingJsonTransform(reqBodyTransform)

        RequestEntity.fromBody(req).as[Message]
          .flatMap { bodyMessage =>
            val pathMessage  = serDeser.parser.fromJson[Message](pathJson)
            val queryMessage = serDeser.parser.fromJson[Message](queryJson)

            handler.handleUnary(
              bodyMessage.merge(pathMessage).merge(queryMessage).build,
              req.headers,
              method
            )
          }
      }
  }
}
