package org.ivovk.connect_rpc_scala.http.grpc

import org.ivovk.connect_rpc_scala.grpc.MethodRegistry
import org.scalatest.funsuite.AnyFunSuite
import test.MethodRegistryTest.*
import test.MethodRegistryTest.MethodRegistryTestServiceGrpc.MethodRegistryTestService

import scala.concurrent.{ExecutionContext, Future}

class MethodRegistryTest extends AnyFunSuite {

  object TestService extends MethodRegistryTestService {
    override def simpleMethod(request: SimpleMethodRequest): Future[SimpleMethodResponse] = ???

    override def httpAnnotationMethod(request: HttpAnnotationMethodRequest): Future[HttpAnnotationMethodResponse] = ???
  }

  test("support simple methods") {
    val service  = MethodRegistryTestService.bindService(TestService, ExecutionContext.global)
    val registry = MethodRegistry(Seq(service))

    val entry = registry.get("test.MethodRegistryTestService", "SimpleMethod")
    assert(entry.isDefined)
    assert(entry.get.name.service == "test.MethodRegistryTestService")
    assert(entry.get.name.method == "SimpleMethod")
  }

  test("parse http annotations") {
    val service  = MethodRegistryTestService.bindService(TestService, ExecutionContext.global)
    val registry = MethodRegistry(Seq(service))

    val entry = registry.get("test.MethodRegistryTestService", "HttpAnnotationMethod")
    assert(entry.isDefined)
    assert(entry.get.httpRule.isDefined)

    val httpRule = entry.get.httpRule.get

    assert(httpRule.getPost == "/v1/test/http_annotation_method")
    assert(httpRule.getBody == "*")
  }

}
