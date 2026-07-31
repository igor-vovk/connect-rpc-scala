package org.ivovk.connect_rpc_scala.http.json

import io.circe.Json

object JsonProcessing {

  type JsonField = (String, Json)

  def mergeFields(a: List[JsonField], b: List[JsonField]): List[JsonField] =
    if a.isEmpty then b
    else if b.isEmpty then a
    else
      a.foldLeft(b) { case (acc, (k, v)) =>
        acc.find(_._1 == k) match {
          case Some((_, v2)) => acc.updated(acc.indexOf((k, v2)), (k, merge(v, v2)))
          case None          => acc :+ (k, v)
        }
      }

  private def merge(a: Json, b: Json): Json =
    (a.asObject, b.asObject, a.asArray, b.asArray, a.asString, b.asString) match
      case (Some(xs), Some(ys), _, _, _, _) =>
        Json.obj(mergeFields(xs.toList, ys.toList)*)
      case (_, _, Some(xs), Some(ys), _, _) => Json.fromValues(xs ++ ys)
      case (_, _, Some(xs), _, _, _)        => Json.fromValues(xs :+ b)
      case (_, _, _, _, Some(x), Some(y))   => Json.arr(Json.fromString(x), Json.fromString(y))
      case _                                => b

  def groupFields(fields: List[JsonField]): List[JsonField] =
    groupFields2(fields.map { (k, v) =>
      if k.contains('.') then k.split('.').toList -> v else List(k) -> v
    })

  private def groupFields2(fields: List[(List[String], Json)]): List[JsonField] =
    fields
      .groupMapReduce((keyParts, _) => keyParts.head) {
        case (_ :: Nil, v)  => List(v)
        case (_ :: tail, v) => List(tail -> v)
        case (Nil, _)       => ???
      }(_ ++ _)
      .view
      .mapValues { fields =>
        if (
          fields.forall {
            case (_: List[String], _: Json) => true
            case _                          => false
          }
        ) {
          Json.obj(groupFields2(fields.asInstanceOf[List[(List[String], Json)]])*)
        } else {
          val jsonValues = fields.asInstanceOf[List[Json]]

          if jsonValues.length == 1 then jsonValues.head
          else Json.fromValues(jsonValues)
        }
      }
      .toList

}
