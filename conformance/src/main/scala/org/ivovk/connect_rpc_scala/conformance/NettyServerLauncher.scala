package org.ivovk.connect_rpc_scala.conformance

import cats.effect.{IO, IOApp}
import connectrpc.conformance.v1.{ConformanceServiceFs2GrpcTrailers, ServerCompatResponse}
import org.ivovk.connect_rpc_scala.ConnectRouteBuilder
import org.ivovk.connect_rpc_scala.conformance.Http4sServerLauncher.getClass
import org.ivovk.connect_rpc_scala.conformance.util.ServerCompatSerDeser
import org.ivovk.connect_rpc_scala.netty.NettyServerBuilder
import org.slf4j.LoggerFactory

/**
 * In short:
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

      server <- NettyServerBuilder
        .forServices[IO](Seq(service))
        .withJsonCodecConfigurator {
          // Registering message types in TypeRegistry is required to pass com.google.protobuf.any.Any
          // JSON-serialization conformance tests
          _
            .registerType[connectrpc.conformance.v1.UnaryRequest]
            .registerType[connectrpc.conformance.v1.IdempotentUnaryRequest]
        }
        .build()

      addr = server.address
      resp = ServerCompatResponse(addr.getHostString, addr.getPort)

      _ <- ServerCompatSerDeser.writeResponse[IO](System.out, resp).toResource

      _ = System.err.println(s"Server started on $addr...")
      _ = logger.info(s"Netty-server started on $addr...")
    yield ()

    res
      .useForever
      .recover { case e =>
        System.err.println(s"Error in server:")
        e.printStackTrace()
      }
  }

}
