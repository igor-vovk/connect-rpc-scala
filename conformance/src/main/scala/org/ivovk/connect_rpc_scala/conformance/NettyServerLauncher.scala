package org.ivovk.connect_rpc_scala.conformance

import cats.effect.{IO, IOApp}
import connectrpc.conformance.v1 as conformance
import fs2.Stream
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import fs2.io.{stdin, stdout}
import org.ivovk.connect_rpc_scala.conformance.util.ProtoCodecs
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
    val res = for
      req <- stdin[IO](2048)
        .through(StreamDecoder.once(ProtoCodecs.decoderFor[conformance.ServerCompatRequest]).toPipeByte)
        .compile.onlyOrError.toResource

      service <- conformance.ConformanceServiceFs2GrpcTrailers.bindServiceResource(
        ConformanceServiceImpl[IO]()
      )

      server <- ConnectNettyServerBuilder
        .forService[IO](service)
        .withJsonCodecConfigurator {
          // Registering message types in TypeRegistry is required to pass com.google.protobuf.any.Any
          // JSON-serialization conformance tests
          _
            .registerType[conformance.UnaryRequest]
            .registerType[conformance.IdempotentUnaryRequest]
        }
        .build()

      _ <- Stream.emit(conformance.ServerCompatResponse(server.host, server.port))
        .through(StreamEncoder.once(ProtoCodecs.encoder).toPipeByte[IO])
        .through(stdout)
        .compile.drain.toResource

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
