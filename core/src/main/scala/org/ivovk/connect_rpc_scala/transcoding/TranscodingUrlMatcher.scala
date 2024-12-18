package org.ivovk.connect_rpc_scala.transcoding

import cats.implicits.*
import com.google.api.http.{CustomHttpPattern, HttpRule}
import org.http4s.{Method, Request, Uri}
import org.ivovk.connect_rpc_scala
import org.ivovk.connect_rpc_scala.grpc.MethodRegistry
import org.ivovk.connect_rpc_scala.http.codec.{AsIsJsonTransform, JsonTransform, SubKeyJsonTransform}
import org.ivovk.connect_rpc_scala.http.json.JsonProcessing.*
import org.json4s.JsonAST.{JField, JObject}
import org.json4s.{JString, JValue}

import scala.jdk.CollectionConverters.*

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
    pattern: Uri.Path,
    reqBodyTransform: JsonTransform,
  )

  sealed trait RouteTree

  case class RootNode(
    children: Vector[RouteTree],
  ) extends RouteTree

  case class Node(
    isVariable: Boolean,
    segment: String,
    children: Vector[RouteTree],
  ) extends RouteTree

  case class Leaf(
    httpMethod: Option[Method],
    method: MethodRegistry.Entry,
    bodyMapper: JsonTransform,
  ) extends RouteTree

  private def mkTree(entries: Seq[Entry]): Vector[RouteTree] = {
    entries.groupByOrd(_.pattern.segments.headOption)
      .flatMap { (maybeSegment, entries) =>
        maybeSegment match {
          case None =>
            entries.map { entry =>
              Leaf(entry.httpMethod, entry.method, entry.reqBodyTransform)
            }
          case Some(head) =>
            val variableDef = this.isVariable(head)
            val segment     =
              if variableDef then
                head.encoded.substring(1, head.encoded.length - 1)
              else head.encoded

            List(
              Node(
                variableDef,
                segment,
                mkTree(entries.map(e => e.copy(pattern = e.pattern.splitAt(1)._2))),
              )
            )
        }
      }
      .toVector
  }

  extension [A](it: Iterable[A]) {
    // groupBy with preserving original ordering
    def groupByOrd[B](f: A => B): Map[B, Vector[A]] = {
      val result = collection.mutable.LinkedHashMap.empty[B, Vector[A]]

      it.foreach { elem =>
        val key = f(elem)
        val vec = result.getOrElse(key, Vector.empty)
        result.update(key, vec :+ elem)
      }

      result.toMap
    }

    // Returns the first element that is Some
    def colFirst[B](f: A => Option[B]): Option[B] = {
      val iter = it.iterator
      while (iter.hasNext) {
        val x = f(iter.next())
        if x.isDefined then return x
      }
      None
    }
  }

  private def isVariable(segment: Uri.Path.Segment): Boolean = {
    val enc    = segment.encoded
    val length = enc.length

    length > 2 && enc(0) == '{' && enc(length - 1) == '}'
  }

  def create[F[_]](
    methods: Seq[MethodRegistry.Entry],
    pathPrefix: Uri.Path,
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
      RootNode(mkTree(entries)),
    )
  }

  private def extractMethodAndPattern(rule: HttpRule): (Option[Method], Uri.Path) = {
    val (method, str) = rule.pattern match
      case HttpRule.Pattern.Get(value) => (Method.GET.some, value)
      case HttpRule.Pattern.Put(value) => (Method.PUT.some, value)
      case HttpRule.Pattern.Post(value) => (Method.POST.some, value)
      case HttpRule.Pattern.Delete(value) => (Method.DELETE.some, value)
      case HttpRule.Pattern.Patch(value) => (Method.PATCH.some, value)
      case HttpRule.Pattern.Custom(CustomHttpPattern(kind, value, _)) if kind == "*" => (none, value)
      case other => throw new RuntimeException(s"Unsupported pattern case $other (Rule: $rule)")

    val path = Uri.Path.unsafeFromString(str).dropEndsWithSlash

    (method, path)
  }

  private def extractRequestBodyTransform(rule: HttpRule): JsonTransform = {
    rule.body match
      case "*" | "" => AsIsJsonTransform
      case fieldName => SubKeyJsonTransform(fieldName)
  }
}

class TranscodingUrlMatcher[F[_]](
  tree: TranscodingUrlMatcher.RootNode,
) {

  import TranscodingUrlMatcher.*

  def matchesRequest(req: Request[F]): Option[MatchedRequest] = {
    def doMatch(node: RouteTree, path: List[Uri.Path.Segment], pathVars: List[JField]): Option[MatchedRequest] = {
      node match {
        case Node(isVariable, patternSegment, children) if path.nonEmpty =>
          val pathSegment = path.head
          val pathTail    = path.tail

          if isVariable then
            val newPatchVars = (patternSegment -> JString(pathSegment.encoded)) :: pathVars

            children.colFirst(doMatch(_, pathTail, newPatchVars))
          else if pathSegment.encoded == patternSegment then
            children.colFirst(doMatch(_, pathTail, pathVars))
          else none
        case Leaf(httpMethod, method, reqBodyTransform) if path.isEmpty && httpMethod.forall(_ == req.method) =>
          val queryParams = req.uri.query.toList.map((k, v) => k -> JString(v.getOrElse("")))

          MatchedRequest(
            method,
            JObject(groupFields(pathVars)),
            JObject(groupFields(queryParams)),
            reqBodyTransform,
          ).some
        case _ => none
      }
    }

    val path = req.uri.path.segments.toList

    tree.children.colFirst(doMatch(_, path, Nil))
  }

}
