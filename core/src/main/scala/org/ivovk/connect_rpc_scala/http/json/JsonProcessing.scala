package org.ivovk.connect_rpc_scala.http.json

import org.json4s.JsonAST.{JField, JObject}
import org.json4s.{JArray, JNothing, JString, JValue}

object JsonProcessing {

  def mergeFields(a: List[JField], b: List[JField]): List[JField] = {
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

  def groupFields(fields: List[JField]): List[JField] = {
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
