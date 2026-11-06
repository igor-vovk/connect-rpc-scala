package org.ivovk.connect_rpc_scala.transcoding

import cats.effect.IO
import com.google.api.http.HttpRule
import io.circe.Json
import org.http4s.implicits.uri
import org.http4s.{Method, Request, Uri}
import org.ivovk.connect_rpc_scala.http.codec.{AsIsJsonTransform, SubKeyJsonTransform}
import org.ivovk.connect_rpc_scala.service.{
  ConnectRequestContext,
  ConnectResponse,
  UnaryMethod,
  UnaryMethodDescriptor,
}
import org.ivovk.connect_rpc_scala.transcoding.TranscodingUrlMatcher.extractVariable
import org.scalatest.funsuite.AnyFunSuiteLike

class TranscodingUrlMatcherTest extends AnyFunSuiteLike {

  private def method(name: String, rule: HttpRule): UnaryMethod[IO] =
    UnaryMethod(
      UnaryMethodDescriptor(
        serviceName = "CountriesService",
        methodName = name,
        requestCompanion = connectrpc.Error,
        responseCompanion = connectrpc.Error,
        httpRule = Some(rule),
      ),
      (request: connectrpc.Error, _: ConnectRequestContext) => IO.pure(ConnectResponse(request)),
    )

  private val matcher = TranscodingUrlMatcher[IO](
    Seq(
      method("CreateCountry", HttpRule().withPost("/countries").withBody("message")),
      method("ListCountries", HttpRule().withGet("/countries/list")),
      method("GetCountry", HttpRule().withGet("/countries/{message}")),
      method("UpdateCountry", HttpRule().withPut("/countries/{message}").withBody("*")),
    ),
    "api" :: Nil,
  )

  private def matching(request: Request[IO]): Option[MatchedRequest[IO]] =
    matcher.matchRequest(
      request.method,
      request.uri.path.segments.map(_.encoded).toList,
      request.uri.query.pairs,
    )

  test("matches request with GET method") {
    val result = matching(Request[IO](Method.GET, uri"/api/countries/list"))

    assert(result.isDefined)
    assert(result.get.method.descriptor.methodName == "ListCountries")
  }

  test("matches request with POST method and body transform") {
    val result = matching(Request[IO](Method.POST, uri"/api/countries"))

    assert(result.isDefined)
    assert(result.get.method.descriptor.methodName == "CreateCountry")
    assert(result.get.reqBodyTransform == SubKeyJsonTransform("message"))
  }

  test("matches request with PUT method") {
    val result = matching(Request[IO](Method.PUT, uri"/api/countries/Uganda"))

    assert(result.isDefined)
    assert(result.get.method.descriptor.methodName == "UpdateCountry")
    assert(result.get.pathJson == Json.obj("message" -> Json.fromString("Uganda")))
    assert(result.get.reqBodyTransform == AsIsJsonTransform)
  }

  test("extracts query parameters") {
    val result = matching(Request[IO](Method.GET, uri"/api/countries/list?limit=10&offset=5"))

    assert(result.isDefined)
    assert(result.get.method.descriptor.methodName == "ListCountries")
    assert(
      result.get.queryJson == Json.obj(
        "limit"  -> Json.fromString("10"),
        "offset" -> Json.fromString("5"),
      )
    )
  }

  test("matches request with path parameter and extracts it") {
    val result = matching(Request[IO](Method.GET, uri"/api/countries/Uganda"))

    assert(result.isDefined)
    assert(result.get.method.descriptor.methodName == "GetCountry")
    assert(result.get.pathJson == Json.obj("message" -> Json.fromString("Uganda")))
  }

  test("extracts repeating query parameters") {
    val result = matching(Request[IO](Method.GET, uri"/api/countries/list?limit=10&limit=20"))

    assert(result.isDefined)
    assert(result.get.method.descriptor.methodName == "ListCountries")
    assert(
      result.get.queryJson == Json.obj(
        "limit" -> Json.arr(Json.fromString("10"), Json.fromString("20"))
      )
    )
  }

  test("extract variable from path segment") {
    assert(extractVariable("{}").isEmpty)

    assert(extractVariable("countries").isEmpty)
    assert(extractVariable("{country_id}").contains("country_id"))
  }

}
