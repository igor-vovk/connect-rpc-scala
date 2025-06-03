package org.ivovk.connect_rpc_scala.conformance

import cats.effect.{IO, IOApp}
import connectrpc.conformance.v1.{
  ConformanceServiceFs2GrpcTrailers,
  ServerCompatRequest,
  ServerCompatResponse,
}
import org.ivovk.connect_rpc_scala.conformance.util.ProtoSerDeser
import org.ivovk.connect_rpc_scala.netty.ConnectNettyServerBuilder
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
object NettyServerLauncher extends IOApp.Simple {

  private val logger = LoggerFactory.getLogger(getClass)

  override def run: IO[Unit] = {
    val protoSerDeser = ProtoSerDeser.systemInOut[IO]

    val res = for
      req <- protoSerDeser.read[ServerCompatRequest].toResource

      service <- ConformanceServiceFs2GrpcTrailers.bindServiceResource(
        ConformanceServiceImpl[IO]()
      )

      server <- ConnectNettyServerBuilder
        .forService[IO](service)
        .withJsonCodecConfigurator {
          // Registering message types in TypeRegistry is required to pass com.google.protobuf.any.Any
          // JSON-serialization conformance tests
          _
            .registerType[connectrpc.conformance.v1.UnaryRequest]
            .registerType[connectrpc.conformance.v1.IdempotentUnaryRequest]
        }
        .build()

      resp = ServerCompatResponse(server.host, server.port)

      _ <- protoSerDeser.write(resp).toResource

      _ = System.err.println(s"Server started on ${server.host}:${server.port}...")
      _ = logger.info(s"Netty-server started on ${server.host}:${server.port}...")
    yield ()

    res
      .useForever
      .recover { case e =>
        System.err.println(s"Error in server:")
        e.printStackTrace()
      }
  }

}
