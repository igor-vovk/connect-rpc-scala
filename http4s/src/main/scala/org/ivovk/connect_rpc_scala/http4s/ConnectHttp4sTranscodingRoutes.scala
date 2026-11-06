package org.ivovk.connect_rpc_scala.http4s

import cats.data.OptionT
import cats.effect.Async
import cats.implicits.*
import org.http4s.{HttpRoutes, Request, Response, Uri}
import org.ivovk.connect_rpc_scala.http.codec.{EntityToDecode, JsonSerdes}
import org.ivovk.connect_rpc_scala.service.MergingBuilder.*
import org.ivovk.connect_rpc_scala.service.ServiceDefinition
import org.ivovk.connect_rpc_scala.transcoding.{MatchedRequest, TranscodingUrlMatcher}

private[http4s] final class ConnectHttp4sTranscodingRoutes[F[_]: Async](
  services: Seq[ServiceDefinition[F]],
  jsonSerdes: JsonSerdes[F],
  pathPrefix: Uri.Path,
  unaryHandler: ConnectHttp4sUnaryHandler[F],
) {

  private val matcher = TranscodingUrlMatcher[F](
    services.flatMap(_.methods),
    pathPrefix.segments.map(_.encoded).toList,
  )

  val routes: HttpRoutes[F] =
    HttpRoutes[F] { request =>
      matchRequest(request).fold(OptionT.none[F, Response[F]]) { matched =>
        OptionT.liftF(handle(request, matched))
      }
    }

  private def matchRequest(request: Request[F]): Option[MatchedRequest[F]] =
    matcher.matchRequest(
      request.method,
      request.uri.path.segments.map(_.encoded).toList,
      request.uri.query.pairs,
    )

  private def handle(request: Request[F], matched: MatchedRequest[F]): F[Response[F]] = {
    val method                                                     = matched.method
    given scalapb.GeneratedMessageCompanion[method.RequestMessage] = method.descriptor.requestCompanion

    val codec   = jsonSerdes.codec.withDecodingJsonTransform(matched.reqBodyTransform)
    val headers = unaryHandler.connectHeaders(request)

    EntityToDecode[F](request.body, headers).as[method.RequestMessage](using summon, codec)
      .compile.onlyOrError
      .map { bodyMessage =>
        val pathMessage  = jsonSerdes.parser.fromJson[method.RequestMessage](matched.pathJson)
        val queryMessage = jsonSerdes.parser.fromJson[method.RequestMessage](matched.queryJson)
        bodyMessage.merge(pathMessage).merge(queryMessage).build
      }
      .adaptError { case error => unaryHandler.decodeError(error) }
      .flatMap(message => unaryHandler.invokeAndEncode(method, message, headers, jsonSerdes.codec))
      .handleErrorWith(error => unaryHandler.errorResponse(error, jsonSerdes.codec))
  }
}
