package org.ivovk.connect_rpc_scala

import cats.implicits.*
import com.google.api.HttpRule
import org.http4s.{Method, Request, Uri}
import org.ivovk.connect_rpc_scala
import org.ivovk.connect_rpc_scala.grpc.MethodRegistry
import org.json4s.JsonAST.{JField, JObject}
import org.json4s.{JString, JValue}

import scala.util.boundary
import scala.util.boundary.break

case class MatchedRequest(method: MethodRegistry.Entry, json: JValue)

object TranscodingUrlMatcher {
  case class Entry(
    method: MethodRegistry.Entry,
    httpMethodMatcher: Method => Boolean,
    pattern: Uri.Path,
  )

  def create[F[_]](
    methods: Seq[MethodRegistry.Entry],
    pathPrefix: Uri.Path,
  ): TranscodingUrlMatcher[F] = {
    val entries = methods.flatMap { method =>
      method.httpRule match {
        case Some(httpRule) =>
          val (httpMethod, pattern) = extractMethodAndPattern(httpRule)

          val httpMethodMatcher: Method => Boolean = m => httpMethod.forall(_ == m)

          Entry(
            method,
            httpMethodMatcher,
            pathPrefix.dropEndsWithSlash.concat(pattern.toRelative)
          ).some
        case None => none
      }
    }

    new TranscodingUrlMatcher(
      entries,
    )
  }

  private def extractMethodAndPattern(rule: HttpRule): (Option[Method], Uri.Path) = {
    val (method, str) = rule.getPatternCase match
      case HttpRule.PatternCase.GET => (Method.GET.some, rule.getGet)
      case HttpRule.PatternCase.PUT => (Method.PUT.some, rule.getPut)
      case HttpRule.PatternCase.POST => (Method.POST.some, rule.getPost)
      case HttpRule.PatternCase.DELETE => (Method.DELETE.some, rule.getDelete)
      case HttpRule.PatternCase.PATCH => (Method.PATCH.some, rule.getPatch)
      case HttpRule.PatternCase.CUSTOM => (none, rule.getCustom.getPath)
      case other => throw new RuntimeException(s"Unsupported pattern case $other (Rule: $rule)")

    val path = Uri.Path.unsafeFromString(str).dropEndsWithSlash

    (method, path)
  }
}

class TranscodingUrlMatcher[F[_]](
  entries: Seq[TranscodingUrlMatcher.Entry],
) {

  import org.ivovk.connect_rpc_scala.http.json.JsonProcessing.*

  def matchesRequest(req: Request[F]): Option[MatchedRequest] = boundary {
    entries.foreach { entry =>
      if (entry.httpMethodMatcher(req.method)) {
        matchExtract(entry.pattern, req.uri.path) match {
          case Some(pathParams) =>
            val queryParams = req.uri.query.toList.map((k, v) => k -> JString(v.getOrElse("")))

            val merged = mergeFields(groupFields(pathParams), groupFields(queryParams))

            break(Some(MatchedRequest(entry.method, JObject(merged))))
          case None => // continue
        }
      }
    }

    none
  }

  /**
   * Matches path segments with pattern segments and extracts variables from the path.
   * Returns None if the path does not match the pattern.
   */
  private def matchExtract(pattern: Uri.Path, path: Uri.Path): Option[List[JField]] = boundary {
    if path.segments.length != pattern.segments.length then boundary.break(none)

    path.segments.indices
      .foldLeft(List.empty[JField]) { (state, idx) =>
        val pathSegment    = path.segments(idx)
        val patternSegment = pattern.segments(idx)

        if isVariable(patternSegment) then
          val varName = patternSegment.encoded.substring(1, patternSegment.encoded.length - 1)

          (varName -> JString(pathSegment.encoded)) :: state
        else if pathSegment != patternSegment then
          boundary.break(none)
        else state
      }
      .some
  }

  private def isVariable(segment: Uri.Path.Segment): Boolean = {
    val enc    = segment.encoded
    val length = enc.length

    length > 2 && enc(0) == '{' && enc(length - 1) == '}'
  }
}
