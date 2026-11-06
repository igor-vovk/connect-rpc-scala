package org.ivovk.connect_rpc_scala.codegen

import com.google.protobuf.DescriptorProtos.{
  DescriptorProto,
  FileDescriptorProto,
  MethodDescriptorProto,
  ServiceDescriptorProto,
}
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import protocgen.CodeGenRequest

class ConnectRpcScalaGeneratorTest extends AnyFunSuite with Matchers {

  test("generates unary service definitions without io.grpc") {
    val response = generate(clientStreaming = false, serverStreaming = false)

    response.hasError shouldBe false
    response.getFileCount shouldBe 1
    response.getFile(0).getContent should include("object ExampleServiceConnect")
    response.getFile(0).getContent should include("trait Service[F[_]]")
    response.getFile(0).getContent should include("UnaryMethodDescriptor")
    response.getFile(0).getContent should not include "io.grpc"
  }

  test("rejects streaming methods explicitly") {
    val response = generate(clientStreaming = false, serverStreaming = true)

    response.hasError shouldBe true
    response.getError should include("Streaming RPCs are not supported yet")
    response.getError should include("test.ExampleService.Call")
  }

  private def generate(clientStreaming: Boolean, serverStreaming: Boolean) = {
    val requestMessage  = DescriptorProto.newBuilder().setName("Request").build()
    val responseMessage = DescriptorProto.newBuilder().setName("Response").build()
    val method          = MethodDescriptorProto.newBuilder()
      .setName("Call")
      .setInputType(".test.Request")
      .setOutputType(".test.Response")
      .setClientStreaming(clientStreaming)
      .setServerStreaming(serverStreaming)
      .build()
    val service = ServiceDescriptorProto.newBuilder()
      .setName("ExampleService")
      .addMethod(method)
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
}
