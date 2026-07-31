package org.ivovk.connect_rpc_scala.http.json

import io.circe.Json
import org.scalatest.funsuite.AnyFunSuiteLike

class JsonProcessingTest extends AnyFunSuiteLike {

  import JsonProcessing.groupFields

  test("groups no fields") {
    assert(groupFields(Nil) == Nil)
  }

  test("preserves flat fields") {
    val fields = List(
      "limit"  -> Json.fromString("10"),
      "offset" -> Json.fromString("5"),
    )

    assert(Json.obj(groupFields(fields)*) == Json.obj(fields*))
  }

  test("groups nested sibling fields") {
    val fields = List(
      "country.address.city"    -> Json.fromString("Kampala"),
      "country.address.country" -> Json.fromString("Uganda"),
    )

    assert(
      Json.obj(groupFields(fields)*) == Json.obj(
        "country" -> Json.obj(
          "address" -> Json.obj(
            "city"    -> Json.fromString("Kampala"),
            "country" -> Json.fromString("Uganda"),
          )
        )
      )
    )
  }

  test("groups repeated flat fields into an array") {
    val fields = List(
      "region" -> Json.fromString("central"),
      "region" -> Json.fromString("western"),
    )

    assert(
      Json.obj(groupFields(fields)*) == Json.obj(
        "region" -> Json.arr(
          Json.fromString("central"),
          Json.fromString("western"),
        )
      )
    )
  }

  test("groups repeated nested fields into an array") {
    val fields = List(
      "country.regions.name" -> Json.fromString("central"),
      "country.regions.name" -> Json.fromString("western"),
    )

    assert(
      Json.obj(groupFields(fields)*) == Json.obj(
        "country" -> Json.obj(
          "regions" -> Json.obj(
            "name" -> Json.arr(
              Json.fromString("central"),
              Json.fromString("western"),
            )
          )
        )
      )
    )
  }

}
