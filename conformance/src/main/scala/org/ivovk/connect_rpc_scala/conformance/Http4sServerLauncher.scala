package org.ivovk.connect_rpc_scala.conformance

import cats.effect.{IO, IOApp}
import com.comcast.ip4s.{host, port, Port}
import connectrpc.conformance.v1.{
  ConformanceServiceFs2GrpcTrailers,
  ServerCompatRequest,
  ServerCompatResponse,
}
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.middleware.Logger
import org.ivovk.connect_rpc_scala.conformance.util.ProtoSerDeser
import org.ivovk.connect_rpc_scala.http4s.ConnectHttp4sRouteBuilder
import org.slf4j.LoggerFactory

/**
 * Flow:
 *
 *   - Upon launch, `ServerCompatRequest` message is sent from the test runner to the server to STDIN.
 *   - Server is started and listens on a random port.
 *   - `ServerCompatResponse` is sent from the server to STDOUT, which instructs the test runner on which port
 *     the server is listening.
 *
 * All diagnostics should be written to STDERR.
 *
 * Useful links:
 *
 * [[https://github.com/connectrpc/conformance/blob/main/docs/configuring_and_running_tests.md]]
 */
object Http4sServerLauncher extends IOApp.Simple {

  private val logger = LoggerFactory.getLogger(getClass)

  override def run: IO[Unit] = {
    val protoSerDeser = ProtoSerDeser.systemInOut[IO]

    val res = for
      req <- protoSerDeser.read[ServerCompatRequest].toResource

      service <- ConformanceServiceFs2GrpcTrailers.bindServiceResource(
        ConformanceServiceImpl[IO]()
      )

      app <- ConnectHttp4sRouteBuilder.forService[IO](service)
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
        logAction = Some(str => IO(this.logger.trace(str))),
      )(app)

      server <- EmberServerBuilder.default[IO]
        .withHost(host"127.0.0.1")
        .withPort(port"0") // random port
        .withHttp2
        .withHttpApp(logger)
        .build

      addr = server.address
      resp = ServerCompatResponse(addr.getHostString, addr.getPort)

      _ <- protoSerDeser.write(resp).toResource

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
