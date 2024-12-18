package org.ivovk.connect_rpc_scala.transcoding

import cats.MonadThrow
import cats.data.OptionT
import cats.implicits.*
import org.http4s.HttpRoutes
import org.ivovk.connect_rpc_scala.grpc.MergingBuilder.*
import org.ivovk.connect_rpc_scala.http.RequestEntity
import org.ivovk.connect_rpc_scala.http.codec.{JsonMessageCodec, MessageCodec}
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

class TranscodingRoutesProvider[F[_] : MonadThrow](
  urlMatcher: TranscodingUrlMatcher[F],
  handler: TranscodingHandler[F],
  jsonCodec: JsonMessageCodec[F]
) {
  private given MessageCodec[F] = jsonCodec

  def routes: HttpRoutes[F] = HttpRoutes[F] { req =>
    OptionT.fromOption[F](urlMatcher.matchesRequest(req))
      .semiflatMap { case MatchedRequest(method, pathJson, queryJson) =>
        given Companion[Message] = method.requestMessageCompanion

        RequestEntity[F](req.body, req.headers).as[Message]
          .flatMap { bodyMessage =>
            val pathMessage  = jsonCodec.parser.fromJson[Message](pathJson)
            val queryMessage = jsonCodec.parser.fromJson[Message](queryJson)

            handler.handleUnary(
              bodyMessage.merge(pathMessage).merge(queryMessage).build,
              req.headers,
              method
            )
          }
      }
  }


}
