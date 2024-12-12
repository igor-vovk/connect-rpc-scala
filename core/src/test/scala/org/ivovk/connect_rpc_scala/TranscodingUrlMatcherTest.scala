package org.ivovk.connect_rpc_scala

import cats.effect.IO
import com.google.api.HttpRule
import org.http4s.Uri.Path.Root
import org.http4s.implicits.uri
import org.http4s.{Method, Request}
import org.ivovk.connect_rpc_scala.grpc.{MethodName, MethodRegistry}
import org.json4s.{JArray, JObject, JString}
import org.scalatest.funsuite.AnyFunSuiteLike

class TranscodingUrlMatcherTest extends AnyFunSuiteLike {

  val matcher = TranscodingUrlMatcher.create[IO](
    Seq(
      MethodRegistry.Entry(
        MethodName("CountriesService", "CreateCountry"),
        null,
        Some(HttpRule.newBuilder().setPost("/countries").build()),
        null
      ),
      MethodRegistry.Entry(
        MethodName("CountriesService", "ListCountries"),
        null,
        Some(HttpRule.newBuilder().setGet("/countries/list").build()),
        null
      ),
      MethodRegistry.Entry(
        MethodName("CountriesService", "GetCountry"),
        null,
        Some(HttpRule.newBuilder().setGet("/countries/{country_id}").build()),
        null
      ),
    ),
    Root / "api"
  )

  test("matches request with GET method") {
    val result = matcher.matchesRequest(Request[IO](Method.GET, uri"/api/countries/list"))

    assert(result.isDefined)
    assert(result.get.method.name == MethodName("CountriesService", "ListCountries"))
    assert(result.get.json == JObject())
  }

  test("matches request with POST method") {
    val result = matcher.matchesRequest(Request[IO](Method.POST, uri"/api/countries"))

    assert(result.isDefined)
    assert(result.get.method.name == MethodName("CountriesService", "CreateCountry"))
    assert(result.get.json == JObject())
  }

  test("extracts query parameters") {
    val result = matcher.matchesRequest(Request[IO](Method.GET, uri"/api/countries/list?limit=10&offset=5"))

    assert(result.isDefined)
    assert(result.get.method.name == MethodName("CountriesService", "ListCountries"))
    assert(result.get.json == JObject("limit" -> JString("10"), "offset" -> JString("5")))
  }

  test("matches request with path parameter and extracts it") {
    val result = matcher.matchesRequest(Request[IO](Method.GET, uri"/api/countries/Uganda"))

    assert(result.isDefined)
    assert(result.get.method.name == MethodName("CountriesService", "GetCountry"))
    assert(result.get.json == JObject("country_id" -> JString("Uganda")))
  }

  test("extracts repeating query parameters") {
    val result = matcher.matchesRequest(Request[IO](Method.GET, uri"/api/countries/list?limit=10&limit=20"))

    assert(result.isDefined)
    assert(result.get.method.name == MethodName("CountriesService", "ListCountries"))
    assert(result.get.json == JObject("limit" -> JArray(JString("10") :: JString("20") :: Nil)))
  }

}
