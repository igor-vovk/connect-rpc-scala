package org.ivovk.connect_rpc_scala

import com.google.api.HttpRule
import org.http4s.Uri.Path
import org.http4s.{Method, Query, Request}
import org.ivovk.connect_rpc_scala.grpc.{MethodName, MethodRegistry}
import org.json4s.JsonAST.JObject
import org.json4s.{JString, JValue}

import scala.util.boundary
import scala.util.boundary.break

case class MatchedRequest(methodName: MethodName, json: JValue)

class GrpcTranscodingUrlMatcher[F[_]](entries: Seq[MethodRegistry.Entry]) {
  def matchesRequest(req: Request[F]): Option[MatchedRequest] =
    boundary {
      entries.foreach { entry =>
        entry.httpRule match {
          case Some(httpRule) =>
            if (matchesHttpMethod(httpRule, req.method) && matchesPath(httpRule, req.uri.path)) {
              break(Some(MatchedRequest(entry.name, extractQueryParams(httpRule, req.uri.path, req.uri.query))))
            }
          case None => // do nothing
        }
      }

      None
    }


  private def matchesHttpMethod(httpRule: HttpRule, method: Method): Boolean = {
    httpRule.getPatternCase match
      case HttpRule.PatternCase.GET => method == Method.GET
      case HttpRule.PatternCase.PUT => method == Method.PUT
      case HttpRule.PatternCase.POST => method == Method.POST
      case HttpRule.PatternCase.DELETE => method == Method.DELETE
      case HttpRule.PatternCase.PATTERN_NOT_SET => true
      case _ => false
  }

  private def matchesPath(httpRule: HttpRule, path: Path): Boolean = {
    val patternParts = extractPattern(httpRule).split("/").filter(_.nonEmpty)

    if path.segments.length != patternParts.length then return false

    path.segments.zip(patternParts).forall { case (pathPart, patternPart) =>
      patternPart.startsWith("{") && patternPart.endsWith("}") || pathPart.encoded == patternPart
    }
  }

  private def extractPattern(httpRule: HttpRule): String = {
    httpRule.getPatternCase match
      case HttpRule.PatternCase.GET => httpRule.getGet
      case HttpRule.PatternCase.PUT => httpRule.getPut
      case HttpRule.PatternCase.POST => httpRule.getPost
      case HttpRule.PatternCase.DELETE => httpRule.getDelete
      case HttpRule.PatternCase.PATTERN_NOT_SET => ""
      case _ => throw new RuntimeException("Unsupported pattern case")
  }

  private def extractQueryParams(httpRule: HttpRule, path: Path, query: Query): JObject = {
    val patternParts = extractPattern(httpRule).split("/").filter(_.nonEmpty)

    val pathArgs =
      if path.segments.length == patternParts.length then
        path.segments.zip(patternParts)
          .filter { case (pathPart, patternPart) =>
            patternPart.startsWith("{") && patternPart.endsWith("}")
          }
          .map { case (pathPart, patternPart) =>
            patternPart.stripPrefix("{").stripSuffix("}") -> pathPart.encoded
          }
          .toMap
      else Map.empty

    JObject((pathArgs ++ query.params).view.mapValues(JString(_)).toList)
  }
}
