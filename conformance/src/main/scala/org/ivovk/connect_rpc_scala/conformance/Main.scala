package org.ivovk.connect_rpc_scala.conformance

import cats.effect.Resource
import com.comcast.ip4s.{Port, host, port}
import connectrpc.conformance.v1.{ServerCompatRequest, ServerCompatResponse}
import fs2.io.net.Network
import org.http4s.ember.server.EmberServerBuilder
import org.ivovk.connect_rpc_scala.ConnectRouteBuilder
import scalapb.json4s.TypeRegistry
import scalapb.zio_grpc.ServiceList
import zio.interop.catz.*
import zio.{Task, ZIO, ZIOAppDefault}

import java.io.InputStream
import java.nio.ByteBuffer

/**
 * In short:
 *
 * - Upon launch, `ServerCompatRequest` message is sent from the test runner to the server to STDIN.
 *
 * - Server is started and listens on a random port.
 *
 * - `ServerCompatResponse` is sent from the server to STDOUT, which instructs the test runner
 * on which port the server is listening.
 *
 * All diagnostics should be written to STDERR.
 *
 * Useful links:
 *
 * [[https://github.com/connectrpc/conformance/blob/main/docs/configuring_and_running_tests.md]]
 */
object Main extends ZIOAppDefault {

  def run: Task[Nothing] = {
    val res = for
      req <- Resource.eval(ServerCompatSerDeser.readRequest(System.in))

      services <- Resource.eval(ServiceList.empty.add(ConformanceServiceImpl).bindAll)

      app <- ConnectRouteBuilder.forServices[Task](services)
        // Registering message types in TypeRegistry is required to pass com.google.protobuf.any.Any
        // JSON-serialization conformance tests
        .withJsonPrinterConfigurator { p =>
          p.withTypeRegistry(
            TypeRegistry.default
              .addMessage[connectrpc.conformance.v1.UnaryRequest]
              .addMessage[connectrpc.conformance.v1.IdempotentUnaryRequest]
              .addMessage[connectrpc.conformance.v1.ConformancePayload.RequestInfo]
          )
        }
        .build

      given Network[Task] = Network.forAsync[Task]

      server <- EmberServerBuilder.default[Task]
        .withHost(host"127.0.0.1")
        .withPort(port"0") // random port
        .withHttpApp(app)
        .build

      addr = server.address
      resp = ServerCompatResponse(addr.getHostString, addr.getPort)

      _ <- Resource.eval(ServerCompatSerDeser.writeResponse(System.out, resp))

      _ = System.err.println(s"Server started on $addr...")
    yield ()

    res.useForever
  }

}

object ServerCompatSerDeser {
  def readRequest(in: InputStream): Task[ServerCompatRequest] =
    ZIO.attempt {
      val length = IntSerDeser.read(in)
      ServerCompatRequest.parseFrom(in.readNBytes(length))
    }

  def writeResponse(out: java.io.OutputStream, resp: ServerCompatResponse): Task[Unit] =
    ZIO.attempt {
      IntSerDeser.write(out, resp.serializedSize)
      out.flush()
      out.write(resp.toByteArray)
      out.flush()
    }
}

object IntSerDeser {
  def read(in: InputStream): Int = ByteBuffer.wrap(in.readNBytes(4)).getInt

  def write(out: java.io.OutputStream, i: Int): Unit = out.write(ByteBuffer.allocate(4).putInt(i).array())
}