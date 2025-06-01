# Integrating with OpenTelemetry

GRPC has its own instrumentation for OpenTelemetry, which is used to capture the traffic going through the GRPC server.
With it, per-endpoint monitoring can be achieved.
List of metrics that are captured can be
found [here](https://grpc.io/docs/guides/opentelemetry-metrics/).

Here is how you can integrate OpenTelemetry with the Connect-RPC server
(example uses `otel4s`, `grpc-opentelemetry` and `cats-effect-simple-di` libraries):

```scala
// Dependencies.scala
object Dependencies {

  def create(runtime: IORuntime): Resource[IO, Dependencies] =
    Allocator.create(runtime).map(new Dependencies(_))

}

class Dependencies(allocator: Allocator) {

  // Create OpenTelemetry instance
  lazy val otel: OtelJava[IO] = allocator.allocate {
    OtelJava.autoConfigured[IO]()
  }

  // Create GRPC OpenTelemetry instance
  lazy val grpcOtel: GrpcOpenTelemetry = {
    GrpcOpenTelemetry.newBuilder()
      .sdk(otel.underlying)
      .build()
  }

  lazy val httpServer: Resource[IO, org.http4s.server.Server] = {
    import com.comcast.ip4s.*

    for {
      // Create httpApp with Connect-RPC routes, specifying your GRPC services
      app <- ConnectHttp4sRouteBuilder
        .forServices[IO](
          // Inject your GRPC services
        )
        .withServerConfigurator { sb =>
          grpcOtel.configureServerBuilder(sb)
          sb
        }
        .build

      // Create http server
      server <- EmberServerBuilder.default[IO]
        .withHost(host"0.0.0.0")
        .withPort(port"8080")
        .withHttpApp(app)
        .build
    } yield server
  }

}

// Main.scala
object Main extends IOApp.Simple {

  override def run: IO[Unit] = {
    Dependencies.create(runtime)
      .flatMap { deps =>
        (
          // deps.grpcServer, <- not listed in the example above, but this is how you can start multiple servers
          deps.httpServer,
        ).parTupled
      }
      .useForever
  }

}
```
