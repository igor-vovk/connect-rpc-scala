package org.ivovk.connect_rpc_scala.conformance

import cats.effect.{IO, IOApp, Sync}
import com.comcast.ip4s.{Port, host, port}
import connectrpc.conformance.v1.{ConformanceServiceFs2GrpcTrailers, ServerCompatRequest, ServerCompatResponse}
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.middleware.Logger
import org.ivovk.connect_rpc_scala.ConnectRouteBuilder
import org.slf4j.LoggerFactory

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

  private val logger = LoggerFactory.getLogger(getClass)

  override def run: IO[Unit] = {
    val res = for
      req <- ServerCompatSerDeser.readRequest[IO](System.in).toResource

      service <- ConformanceServiceFs2GrpcTrailers.bindServiceResource(
        ConformanceServiceImpl[IO]()
      )

      app <- ConnectRouteBuilder.forService[IO](service)
        .withJsonCodecConfigurator {
          // Registering message types in TypeRegistry is required to pass com.google.protobuf.any.Any
          // JSON-serialization conformance tests
          _
            .registerType[connectrpc.conformance.v1.UnaryRequest]
            .registerType[connectrpc.conformance.v1.IdempotentUnaryRequest]
        }
        .build

      logger = Logger.httpApp[IO](
        logHeaders = false,
        logBody = false,
        logAction = Some(str => IO(this.logger.trace(str)))
      )(app)

      server <- EmberServerBuilder.default[IO]
        .withHost(host"127.0.0.1")
        .withPort(port"0") // random port
        .withHttp2
        .withHttpApp(logger)
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