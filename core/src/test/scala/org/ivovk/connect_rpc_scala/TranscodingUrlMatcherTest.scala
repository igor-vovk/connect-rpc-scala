package org.ivovk.connect_rpc_scala

import cats.effect.IO
import com.google.api.http.HttpRule
import org.http4s.Uri.Path.Root
import org.http4s.implicits.uri
import org.http4s.{Method, Request}
import org.ivovk.connect_rpc_scala.grpc.{MethodName, MethodRegistry}
import org.json4s.{JArray, JObject, JString}
import org.scalatest.funsuite.AnyFunSuiteLike

class TranscodingUrlMatcherTest extends AnyFunSuiteLike {

  private val matcher = TranscodingUrlMatcher.create[IO](
    Seq(
      MethodRegistry.Entry(
        MethodName("CountriesService", "CreateCountry"),
        null,
        Some(HttpRule().withPost("/countries")),
        null
      ),
      MethodRegistry.Entry(
        MethodName("CountriesService", "ListCountries"),
        null,
        Some(HttpRule().withGet("/countries/list")),
        null
      ),
      MethodRegistry.Entry(
        MethodName("CountriesService", "GetCountry"),
        null,
        Some(HttpRule().withGet("/countries/{country_id}")),
        null
      ),
    ),
    Root / "api"
  )

  test("matches request with GET method") {
    val result = matcher.matchesRequest(Request[IO](Method.GET, uri"/api/countries/list"))

    assert(result.isDefined)
    assert(result.get.method.name == MethodName("CountriesService", "ListCountries"))
  }

  test("matches request with POST method") {
    val result = matcher.matchesRequest(Request[IO](Method.POST, uri"/api/countries"))

    assert(result.isDefined)
    assert(result.get.method.name == MethodName("CountriesService", "CreateCountry"))
  }

  test("extracts query parameters") {
    val result = matcher.matchesRequest(Request[IO](Method.GET, uri"/api/countries/list?limit=10&offset=5"))

    assert(result.isDefined)
    assert(result.get.method.name == MethodName("CountriesService", "ListCountries"))
    assert(result.get.queryJson == JObject("limit" -> JString("10"), "offset" -> JString("5")))
  }

  test("matches request with path parameter and extracts it") {
    val result = matcher.matchesRequest(Request[IO](Method.GET, uri"/api/countries/Uganda"))

    assert(result.isDefined)
    assert(result.get.method.name == MethodName("CountriesService", "GetCountry"))
    assert(result.get.pathJson == JObject("country_id" -> JString("Uganda")))
  }

  test("extracts repeating query parameters") {
    val result = matcher.matchesRequest(Request[IO](Method.GET, uri"/api/countries/list?limit=10&limit=20"))

    assert(result.isDefined)
    assert(result.get.method.name == MethodName("CountriesService", "ListCountries"))
    assert(result.get.queryJson == JObject("limit" -> JArray(JString("10") :: JString("20") :: Nil)))
  }

}
