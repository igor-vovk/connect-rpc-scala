package org.ivovk.connect_rpc_scala.http.json

import io.circe.Json

object JsonProcessing {

  type JsonField = (String, Json)

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
