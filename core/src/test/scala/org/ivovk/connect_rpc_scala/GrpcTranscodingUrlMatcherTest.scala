package org.ivovk.connect_rpc_scala

import cats.effect.IO
import com.google.api.HttpRule
import org.http4s.implicits.uri
import org.http4s.{Method, Request}
import org.ivovk.connect_rpc_scala.grpc.{MethodName, MethodRegistry}
import org.json4s.{JObject, JString}
import org.scalatest.funsuite.AnyFunSuiteLike

class GrpcTranscodingUrlMatcherTest extends AnyFunSuiteLike {

  val matcher = new GrpcTranscodingUrlMatcher[IO](Seq(
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
  ))

  test("matches simple request") {
    val result = matcher.matchesRequest(Request[IO](Method.GET, uri"/countries/list"))

    assert(result.isDefined)
    assert(result.get.methodName == MethodName("CountriesService", "ListCountries"))
    assert(result.get.json == JObject())
  }

  test("extracts query parameters") {
    val result = matcher.matchesRequest(Request[IO](Method.GET, uri"/countries/list?limit=10&offset=5"))

    assert(result.isDefined)
    assert(result.get.methodName == MethodName("CountriesService", "ListCountries"))
    assert(result.get.json == JObject("limit" -> JString("10"), "offset" -> JString("5")))
  }

  test("matches request with path parameter") {
    val result = matcher.matchesRequest(Request[IO](Method.GET, uri"/countries/Uganda"))

    assert(result.isDefined)
    assert(result.get.methodName == MethodName("CountriesService", "GetCountry"))
    assert(result.get.json == JObject("country_id" -> JString("Uganda")))
  }

  test("matches request with POST method") {
    val result = matcher.matchesRequest(Request[IO](Method.POST, uri"/countries"))

    assert(result.isDefined)
    assert(result.get.methodName == MethodName("CountriesService", "CreateCountry"))
    assert(result.get.json == JObject())
  }

}
