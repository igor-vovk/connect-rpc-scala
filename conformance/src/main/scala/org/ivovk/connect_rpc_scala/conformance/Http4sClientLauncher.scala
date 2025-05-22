package org.ivovk.connect_rpc_scala.conformance

import cats.effect.{IO, IOApp}
import connectrpc.conformance.v1.{
  ClientCompatRequest,
  ClientCompatResponse,
  ConformanceServiceFs2GrpcTrailers,
}
import org.ivovk.connect_rpc_scala.conformance.util.ProtoSerDeser

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
object Http4sClientLauncher extends IOApp.Simple {

//  private val logger = LoggerFactory.getLogger(getClass)

  override def run: IO[Unit] =
    fs2.Stream.repeatEval {
      val f = for
        req <- ProtoSerDeser[IO].read[ClientCompatRequest](System.in).toResource

        service <- ConformanceServiceFs2GrpcTrailers.bindServiceResource(
          ConformanceServiceImpl[IO]()
        )

        resp = ClientCompatResponse(req.testName)

        _ <- ProtoSerDeser[IO].write(System.out, resp).toResource
      yield ()

      f.use_
    }.compile.drain

}
