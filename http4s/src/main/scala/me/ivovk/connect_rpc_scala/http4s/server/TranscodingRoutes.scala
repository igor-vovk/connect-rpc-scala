package me.ivovk.connect_rpc_scala.http4s.server

import cats.data.OptionT
import cats.effect.Async
import cats.implicits.*
import org.http4s.{ContentCoding, HttpRoutes, Request, Response, Uri}
import me.ivovk.connect_rpc_scala.http.codec.{DecodeOptions, JsonSerdes}
import me.ivovk.connect_rpc_scala.transcoding.MergingBuilder.*
import me.ivovk.connect_rpc_scala.ServiceDefinition
import me.ivovk.connect_rpc_scala.transcoding.{MatchedRequest, TranscodingUrlMatcher}
import scalapb.{GeneratedMessageCompanion => Companion}

private[http4s] final class TranscodingRoutes[F[_]: Async](
  services: Seq[ServiceDefinition[F]],
  jsonSerdes: JsonSerdes[F],
  pathPrefix: Uri.Path,
  unaryHandler: UnaryHandler[F],
) {

  private val matcher = TranscodingUrlMatcher.create[F](
    services.flatMap(_.unaryMethods),
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
    val method = matched.method

    given Companion[method.RequestMessage] = method.descriptor.requestCompanion

    val codec    = jsonSerdes.codec.withDecodingJsonTransform(matched.reqBodyTransform)
    val headers  = request.headers
    val encoding = ContentCoding.identity

    codec.decode[method.RequestMessage](request.body, DecodeOptions.fromHeaders(headers))
      .map { bodyMessage =>
        val pathMessage  = jsonSerdes.parser.fromJson[method.RequestMessage](matched.pathJson)
        val queryMessage = jsonSerdes.parser.fromJson[method.RequestMessage](matched.queryJson)
        bodyMessage.merge(pathMessage).merge(queryMessage).build
      }
      .adaptError { case error => unaryHandler.decodeError(error) }
      .flatMap { message =>
        unaryHandler.invokeAndEncode(method, message, headers, jsonSerdes.codec, encoding)
      }
      .handleErrorWith(error => unaryHandler.errorResponse(error, encoding))
  }
}
