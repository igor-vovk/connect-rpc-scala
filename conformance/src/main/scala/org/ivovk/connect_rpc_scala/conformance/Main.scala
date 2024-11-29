package org.ivovk.connect_rpc_scala.conformance

import cats.effect.{IO, IOApp, Sync}
import com.comcast.ip4s.{Port, host, port}
import connectrpc.conformance.v1.{ConformanceServiceFs2GrpcTrailers, ServerCompatRequest, ServerCompatResponse}
import org.http4s.ember.server.EmberServerBuilder
import org.ivovk.connect_rpc_scala.{Configuration, ConnectRpcHttpRoutes}
import scalapb.json4s.TypeRegistry

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
object Main extends IOApp.Simple {

  override def run: IO[Unit] = {
    val res = for
      req <- ServerCompatSerDeser.readRequest[IO](System.in).toResource

      service <- ConformanceServiceFs2GrpcTrailers.bindServiceResource(
        ConformanceServiceImpl[IO]()
      )

      httpApp <- ConnectRpcHttpRoutes
        .create[IO](
          Seq(service),
          Configuration(
            jsonPrinterConfiguration = { p =>
              // Registering message types in TypeRegistry is required to pass com.google.protobuf.any.Any
              // JSON-serialization conformance tests
              p.withTypeRegistry(
                TypeRegistry.default
                  .addMessage[connectrpc.conformance.v1.UnaryRequest]
                  .addMessage[connectrpc.conformance.v1.IdempotentUnaryRequest]
                  .addMessage[connectrpc.conformance.v1.ConformancePayload.RequestInfo]
              )
            }
          )
        )
        .map(r => r.orNotFound)

      server <- EmberServerBuilder.default[IO]
        .withHost(host"127.0.0.1")
        .withPort(port"0") // random port
        .withHttpApp(httpApp)
        .build

      addr = server.address
      resp = ServerCompatResponse(addr.getHostString, addr.getPort)

      _ <- ServerCompatSerDeser.writeResponse[IO](System.out, resp).toResource

      _ = System.err.println(s"Server started on $addr...")
    yield ()

    res
      .useForever
      .recover { case e =>
        System.err.println(s"Error in server:")
        e.printStackTrace()
      }
  }

}

object ServerCompatSerDeser {
  def readRequest[F[_] : Sync](in: InputStream): F[ServerCompatRequest] =
    Sync[F].delay {
      val length = IntSerDeser.read(in)
      ServerCompatRequest.parseFrom(in.readNBytes(length))
    }

  def writeResponse[F[_] : Sync](out: java.io.OutputStream, resp: ServerCompatResponse): F[Unit] =
    Sync[F].delay {
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