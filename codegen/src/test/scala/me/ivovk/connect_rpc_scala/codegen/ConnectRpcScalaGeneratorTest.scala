package me.ivovk.connect_rpc_scala.codegen

import com.google.api.http.HttpRule
import com.google.protobuf.ByteString
import com.google.protobuf.DescriptorProtos.{
  DescriptorProto,
  FileDescriptorProto,
  MethodDescriptorProto,
  MethodOptions,
  ServiceDescriptorProto,
}
import com.google.protobuf.UnknownFieldSet
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import protocgen.CodeGenRequest

import scala.collection.JavaConverters._
import scala.io.Source

class ConnectRpcScalaGeneratorTest extends AnyFunSuite with Matchers {

  test("matches the generated service golden file") {
    val response = generate(
      method(
        "Call",
        httpRule = Some(HttpRule(body = "*").withPost("/v1/add")),
      ),
      method("HealthCheck"),
    )

    response.hasError shouldBe false
    response.getFileCount shouldBe 1
    response.getFile(0).getName shouldBe "test/example/ExampleServiceConnect.scala"
    response.getFile(0).getContent shouldBe golden("ExampleServiceConnect.scala")
  }

  test("generates unary service definitions without io.grpc") {
    val response = generate(method("Call"))

    response.hasError shouldBe false
    response.getFileCount shouldBe 1
    response.getFile(0).getContent should include("object ExampleServiceConnect")
    response.getFile(0).getContent should include("trait Service[F[_], Context]")
    response.getFile(0).getContent should include("UnaryMethodDescriptor")
    response.getFile(0).getContent should not include "io.grpc"
  }

  test("generates unary methods and skips streaming methods in the same service") {
    val response = generate(
      method("Call"),
      method("ServerStream", serverStreaming = true),
      method("ClientStream", clientStreaming = true),
    )

    response.hasError shouldBe false
    response.getFileCount shouldBe 1
    response.getFile(0).getContent should include("METHOD_CALL")
    response.getFile(0).getContent should not include "ServerStream"
    response.getFile(0).getContent should not include "ClientStream"
  }

  test("does not generate a service that contains only streaming methods") {
    val response = generate(method("BidiStream", clientStreaming = true, serverStreaming = true))

    response.hasError shouldBe false
    response.getFileCount shouldBe 0
  }

  private def method(
    name: String,
    clientStreaming: Boolean = false,
    serverStreaming: Boolean = false,
    httpRule: Option[HttpRule] = None,
  ): MethodDescriptorProto = {
    val builder = MethodDescriptorProto.newBuilder()
      .setName(name)
      .setInputType(".test.Request")
      .setOutputType(".test.Response")
      .setClientStreaming(clientStreaming)
      .setServerStreaming(serverStreaming)

    httpRule.foreach { rule =>
      val field = UnknownFieldSet.Field.newBuilder()
        .addLengthDelimited(ByteString.copyFrom(rule.toByteArray))
        .build()
      val unknownFields = UnknownFieldSet.newBuilder()
        .addField(72295728, field)
        .build()
      builder.setOptions(MethodOptions.newBuilder().setUnknownFields(unknownFields))
    }

    builder.build()
  }

  private def generate(methods: MethodDescriptorProto*) = {
    val requestMessage  = DescriptorProto.newBuilder().setName("Request").build()
    val responseMessage = DescriptorProto.newBuilder().setName("Response").build()
    val service         = ServiceDescriptorProto.newBuilder()
      .setName("ExampleService")
      .addAllMethod(methods.asJava)
      .build()
    val file = FileDescriptorProto.newBuilder()
      .setName("example.proto")
      .setPackage("test")
      .setSyntax("proto3")
      .addMessageType(requestMessage)
      .addMessageType(responseMessage)
      .addService(service)
      .build()
    val request = CodeGeneratorRequest.newBuilder()
      .addFileToGenerate(file.getName)
      .addProtoFile(file)
      .build()

    ConnectRpcScalaGenerator.process(CodeGenRequest(request)).toCodeGeneratorResponse
  }

  private def golden(name: String): String = {
    val source = Source.fromResource(s"golden/$name")
    try source.mkString.replace("\r\n", "\n")
    finally source.close()
  }
}
