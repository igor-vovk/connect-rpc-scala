package org.ivovk.connect_rpc_scala.transcoding

import cats.implicits.*
import com.google.api.http.{CustomHttpPattern, HttpRule}
import org.http4s.{Method, Uri}
import org.ivovk.connect_rpc_scala
import org.ivovk.connect_rpc_scala.grpc.MethodRegistry
import org.ivovk.connect_rpc_scala.http.Paths.{Path, Segment}
import org.ivovk.connect_rpc_scala.http.codec.{AsIsJsonTransform, JsonTransform, SubKeyJsonTransform}
import org.ivovk.connect_rpc_scala.http.json.JsonProcessing.*
import org.ivovk.connect_rpc_scala.util.SeqOps.*
import org.json4s.JsonAST.{JField, JObject}
import org.json4s.{JString, JValue}

import scala.collection.immutable.ArraySeq

case class MatchedRequest(
  method: MethodRegistry.Entry,
  pathJson: JValue,
  queryJson: JValue,
  reqBodyTransform: JsonTransform,
)

object TranscodingUrlMatcher {
  case class Entry(
    method: MethodRegistry.Entry,
    httpMethod: Option[Method],
    pattern: Path,
    reqBodyTransform: JsonTransform,
  )

  sealed trait RouteTree

  case class RootNode(
    children: IndexedSeq[RouteTree]
  ) extends RouteTree

  case class Node(
    isVariable: Boolean,
    segment: Segment,
    children: IndexedSeq[RouteTree],
  ) extends RouteTree

  case class Leaf(
    entry: Entry
  ) extends RouteTree

  private def mkTree(entries: Seq[Entry]): IndexedSeq[RouteTree] =
    entries
      .groupByPreservingOrdering(_.pattern.headOption)
      .flatMap { (maybeSegment, entries) =>
        maybeSegment match {
          case None =>
            entries.map(Leaf(_))
          case Some(head) =>
            val maybeVariable = extractVariable(head)
            val segment = maybeVariable match
              case Some(variable) => variable
              case None           => head

            List(
              Node(
                isVariable = maybeVariable.isDefined,
                segment = segment,
                children = mkTree(entries.map(e => e.copy(pattern = e.pattern.splitAt(1)._2))),
              )
            )
        }
      }
      .to(ArraySeq)

  def apply[F[_]](
    methods: Seq[MethodRegistry.Entry],
    pathPrefix: Path = Nil,
  ): TranscodingUrlMatcher[F] = {
    val entries = methods.flatMap { method =>
      method.httpRule.fold(List.empty[Entry]) { httpRule =>
        val additionalBindings = httpRule.additionalBindings.toList

        (httpRule :: additionalBindings).map { rule =>
          val (httpMethod, pattern) = extractMethodAndPattern(rule)
          val bodyTransform         = extractRequestBodyTransform(rule)

          Entry(
            method,
            httpMethod,
            pathPrefix.concat(pattern),
            bodyTransform,
          )
        }
      }
    }

    new TranscodingUrlMatcher(
      RootNode(mkTree(entries))
    )
  }

  private def extractMethodAndPattern(rule: HttpRule): (Option[Method], Path) = {
    val (method, str) = rule.pattern match
      case HttpRule.Pattern.Get(value) =>
        (Method.GET.some, value)
      case HttpRule.Pattern.Put(value) =>
        (Method.PUT.some, value)
      case HttpRule.Pattern.Post(value) =>
        (Method.POST.some, value)
      case HttpRule.Pattern.Delete(value) =>
        (Method.DELETE.some, value)
      case HttpRule.Pattern.Patch(value) =>
        (Method.PATCH.some, value)
      case HttpRule.Pattern.Custom(CustomHttpPattern(kind, value, _)) if kind == "*" =>
        (none, value)
      case other =>
        throw new RuntimeException(s"Unsupported pattern case $other (Rule: $rule)")

    val path = Uri.Path.unsafeFromString(str).segments.map(_.encoded).toList

    (method, path)
  }

  private[connect_rpc_scala] def extractVariable(segment: Segment): Option[String] = {
    val length = segment.length

    if length > 2 && segment(0) == '{' && segment(length - 1) == '}' then
      segment.substring(1, length - 1).some
    else none
  }

  private def extractRequestBodyTransform(rule: HttpRule): JsonTransform =
    rule.body match
      case "*" | ""  => AsIsJsonTransform
      case fieldName => SubKeyJsonTransform(fieldName)
}

class TranscodingUrlMatcher[F[_]](
  tree: TranscodingUrlMatcher.RootNode
) {

  import TranscodingUrlMatcher.*

  def matchRequest(
    method: Method,
    path: Path,
    query: Seq[(String, Option[String])] = Seq.empty,
  ): Option[MatchedRequest] = {
    def doMatch(
      node: RouteTree,
      path: Path,
      pathVars: List[JField],
    ): Option[MatchedRequest] =
      node match {
        case Node(isVariable, patternSegment, children) if path.nonEmpty =>
          val pathSegment = path.head
          val pathTail    = path.tail

          if isVariable then
            val newPatchVars = (patternSegment -> JString(pathSegment)) :: pathVars

            children.colFirst(doMatch(_, pathTail, newPatchVars))
          else if pathSegment == patternSegment then children.colFirst(doMatch(_, pathTail, pathVars))
          else none
        case Leaf(entry) if path.isEmpty && entry.httpMethod.forall(_ == method) =>
          val queryParams = query.map((k, v) => k -> JString(v.getOrElse(""))).toList

          MatchedRequest(
            entry.method,
            JObject(groupFields(pathVars)),
            JObject(groupFields(queryParams)),
            entry.reqBodyTransform,
          ).some
        case _ => none
      }

    tree.children.colFirst(doMatch(_, path, Nil))
  }

}
