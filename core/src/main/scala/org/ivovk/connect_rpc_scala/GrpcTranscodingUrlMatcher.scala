package org.ivovk.connect_rpc_scala

import com.google.api.HttpRule
import org.http4s.{Method, Request, Uri}
import org.ivovk.connect_rpc_scala.grpc.{MethodName, MethodRegistry}
import org.json4s.JsonAST.{JField, JObject}
import org.json4s.{JArray, JNothing, JString, JValue}

import scala.util.boundary
import scala.util.boundary.break

case class MatchedRequest(methodName: MethodName, json: JValue)

object GrpcTranscodingUrlMatcher {
  private def mergeFields(a: List[JField], b: List[JField]): List[JField] = {
    if a.isEmpty then b
    else if b.isEmpty then a
    else a.foldLeft(b) { case (acc, (k, v)) =>
      acc.find(_._1 == k) match {
        case Some((_, v2)) => acc.updated(acc.indexOf((k, v2)), (k, merge(v, v2)))
        case None => acc :+ (k, v)
      }
    }
  }

  private def merge(a: JValue, b: JValue): JValue = {
    (a, b) match
      case (JObject(xs), JObject(ys)) => JObject(mergeFields(xs, ys))
      case (JArray(xs), JArray(ys)) => JArray(xs ++ ys)
      case (JArray(xs), y) => JArray(xs :+ y)
      case (JNothing, x) => x
      case (x, JNothing) => x
      case (JString(x), JString(y)) => JArray(List(JString(x), JString(y)))
      case (_, y) => y
  }

  private def groupFields(fields: List[JField]): List[JField] = {
    groupFields2(fields.map { (k, v) =>
      if k.contains('.') then k.split('.').toList -> v else List(k) -> v
    })
  }

  private def groupFields2(fields: List[(List[String], JValue)]): List[JField] = {
    fields
      .groupMapReduce((keyParts, _) => keyParts.head) {
        case (_ :: Nil, v) => List(v)
        case (_ :: tail, v) => List(tail -> v)
        case (Nil, _) => ???
      }(_ ++ _)
      .view.mapValues { fields =>
        if (fields.forall {
          case (list: List[String], v: JValue) => true
          case _ => false
        }) {
          JObject(groupFields2(fields.asInstanceOf[List[(List[String], JValue)]]))
        } else {
          val jvalues = fields.asInstanceOf[List[JValue]]

          if jvalues.length == 1 then jvalues.head
          else JArray(jvalues)
        }
      }
      .toList
  }
}

class GrpcTranscodingUrlMatcher[F[_]](
  entries: Seq[MethodRegistry.Entry],
  pathPrefix: Uri.Path,
) {

  import GrpcTranscodingUrlMatcher.*

  def matchesRequest(req: Request[F]): Option[MatchedRequest] = boundary {
    entries.foreach { entry =>
      entry.httpRule match {
        case Some(httpRule) =>
          val pattern = pathPrefix.dropEndsWithSlash.concat(extractPattern(httpRule).toRelative)

          if (matchesHttpMethod(httpRule, req.method)) {
            matchExtract(pattern, req.uri.path) match {
              case Some(pathParams) =>
                val queryParams = req.uri.query.toList.map((k, v) => k -> JString(v.getOrElse("")))

                val merged = mergeFields(groupFields(pathParams), groupFields(queryParams))

                break(Some(MatchedRequest(entry.name, JObject(merged))))
              case None => // continue
            }
          }
        case None => // continue
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

  /**
   * Matches path segments with pattern segments and extracts variables from the path.
   * Returns None if the path does not match the pattern.
   */
  private def matchExtract(pattern: Uri.Path, path: Uri.Path): Option[List[JField]] = {
    if path.segments.length != pattern.segments.length then return None

    boundary {
      val extractedVars = path.segments.zip(pattern.segments)
        .foldLeft(List.empty[JField]) { case (state, (pathSegment, patternSegment)) =>
          if isVariable(patternSegment) then
            val varName = patternSegment.encoded.stripPrefix("{").stripSuffix("}")

            (varName -> JString(pathSegment.encoded)) :: state
          else if pathSegment != patternSegment then
            break(None)
          else state
        }

      Some(extractedVars)
    }
  }

  private def extractPattern(httpRule: HttpRule): Uri.Path = {
    val str = httpRule.getPatternCase match
      case HttpRule.PatternCase.GET => httpRule.getGet
      case HttpRule.PatternCase.PUT => httpRule.getPut
      case HttpRule.PatternCase.POST => httpRule.getPost
      case HttpRule.PatternCase.DELETE => httpRule.getDelete
      case HttpRule.PatternCase.PATTERN_NOT_SET => ""
      case _ => throw new RuntimeException("Unsupported pattern case")

    Uri.Path.unsafeFromString(str).dropEndsWithSlash
  }

  private def isVariable(segment: Uri.Path.Segment): Boolean = {
    val enc    = segment.encoded
    val length = enc.length

    length > 2 && enc(0) == '{' && enc(length - 1) == '}'
  }
}
