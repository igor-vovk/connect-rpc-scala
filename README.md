![](docs/connect-rpc-scala-logo.png)

# Connect RPC for Scala

`connect-rpc-scala` generates unary Connect service APIs from protobuf definitions and serves or calls them with
http4s. ScalaPB still generates the message classes, while this project's generator owns the service definitions.
The runtime does not depend on grpc-java, ScalaPB's gRPC runtime, fs2-grpc, or zio-grpc.

Streaming RPCs are intentionally not supported yet. The generator skips streaming methods while still generating the
unary methods in the same service; a streaming-only service produces no Connect API.

## Features

- Unary Connect endpoints using JSON or protobuf encoding.
- Generated effect-polymorphic service interfaces, binders, and clients.
- Connect request and response headers, trailers, timeouts, and structured errors.
- Identity and gzip request decompression; responses currently use identity encoding.
- HTTP transcoding from `google.api.http` annotations, including GET, POST, PUT, DELETE, PATCH, path parameters,
  repeated query parameters, `body: "*"`, and request-body field mappings.
- An http4s server and client implementation.

## Modules

- `connect-rpc-scala-core`: protocol models, codecs, service descriptors, errors, and transcoding.
- `connect-rpc-scala-http4s`: unary server routes and client transport.
- [`connect-rpc-scala-opentelemetry`](opentelemetry/README.md): opt-in OpenTelemetry metrics for unary server calls.
- `protoc-gen-connect-rpc-scala`: the executable protoc service-definition generator.
- `sbt-connect-rpc-scala`: sbt integration for ScalaPB and Connect source generation.

## Build setup

Add the Connect plugin to `project/plugins.sbt`:

```scala
addSbtPlugin("me.ivovk" % "sbt-connect-rpc-scala" % "<version>")
```

The plugin is cross-published for sbt 1.x and sbt 2.x; sbt selects the matching artifact automatically.

Enable it on each project that owns protobuf definitions:

```scala
lazy val apiProto = project
  .enablePlugins(ConnectRpcScalaPlugin)
```

The plugin generates ScalaPB messages without gRPC services, generates the Connect service definitions, and adds the
matching `connect-rpc-scala-core` and ScalaPB runtime dependencies. Use `connectRpcScalaGeneratorOptions` to append or
replace generator options when needed.

Add the runtime to the application:

```scala
libraryDependencies ++= Seq(
  "me.ivovk" %% "connect-rpc-scala-http4s" % "<version>",
  "com.google.api.grpc" % "proto-google-common-protos" % "2.78.0" % "protobuf",
  "org.http4s" %% "http4s-ember-server" % "0.23.38",
  "org.http4s" %% "http4s-ember-client" % "0.23.38",
)
```

## Define and implement a service

Given this protobuf definition:

```protobuf
syntax = "proto3";

package example.v1;

import "google/api/annotations.proto";

service CalculatorService {
  rpc Add(AddRequest) returns (AddResponse) {
    option (google.api.http) = {
      post: "/v1/add"
      body: "*"
    };
  }
}

message AddRequest {
  int32 left = 1;
  int32 right = 2;
}

message AddResponse {
  int32 result = 1;
}
```

the generator creates `CalculatorServiceConnect.Service[F, Context]`, `bind`, method descriptors, and
`CalculatorServiceConnect.Client[F]`:

```scala
object Calculator extends CalculatorServiceConnect.Service[IO, ConnectRequestContext] {
  override def add(
    request: AddRequest,
    ctx: ConnectRequestContext,
  ): IO[ConnectResponse[AddResponse]] =
    IO.pure(ConnectResponse(AddResponse(request.left + request.right)))
}
```

The service is available both at the Connect procedure and at the annotated transcoding route:

```text
POST /example.v1.CalculatorService/Add
POST /v1/add
```

## http4s server

```scala
val service = CalculatorServiceConnect.bind(Calculator)

val server: Resource[IO, org.http4s.server.Server] =
  for {
    app <- ConnectHttp4sRouteBuilder.forService(service).build
    server <- EmberServerBuilder.default[IO]
      .withHost(host"0.0.0.0")
      .withPort(port"8080")
      .withHttpApp(app)
      .build
  } yield server
```

Use `buildRoutes` when the Connect routes need to be combined with other routes or http4s middleware. See
[CORS support](docs/supporting-cors.md) for an example.

Unary services can also expose Kubernetes health endpoints through transcoding; see
[Kubernetes health checks](docs/kubernetes-health-checks.md).

## Unary interceptors

Import interceptors from `me.ivovk.connect_rpc_scala.interceptors`.
`UnaryInterceptor[F, In, Out]` wraps a unary call and can transform its context from `In` to `Out`.
Pass an interceptor to the generated server's `bind(service, interceptor)` or to the generated
client's `withInterceptor(interceptor)`. Use `first.andThen(second)` to run `first` before `second`,
passing the first interceptor's output context to the second.

The `UnaryInterceptor` companion provides these helpers.

- `identity` forwards the context as-is.
- `transformContext` runs an effectful function to produce the downstream context.
- `tapError` runs a callback with the method descriptor and exception when a call fails. If the
  callback succeeds, the call propagates its original exception.

### RPC error logging

`ErrorLoggingInterceptor` is available in `connect-rpc-scala-core`. It logs the method, Connect
code, and original exception for failed unary calls. The default filter logs codes that map to HTTP
5xx, including unexpected exceptions reported as `INTERNAL`. Client errors such as `INVALID_ARGUMENT`
and `CANCELED` are skipped. The default logger uses SLF4J at `ERROR` level. The interceptor works with
any effect that has a Cats `MonadThrow` instance.

```scala
import me.ivovk.connect_rpc_scala.interceptors.ErrorLoggingInterceptor

val logging = ErrorLoggingInterceptor.builder[IO].build[ConnectRequestContext]()
val service = CalculatorServiceConnect.bind(Calculator, logging)
```

`withLogger` accepts an `org.slf4j.Logger` or a function of type `(String, Throwable) => F[Unit]`.
Use `withCodeFilter` to choose which Connect codes to log. A log4cats logger can be used through a function.

```scala
val logging = ErrorLoggingInterceptor.builder[IO]
  .withLogger((message, cause) => appLogger.error(cause)(message))
  .build[ConnectRequestContext]()
```

### OpenTelemetry metrics

`OpenTelemetryServerInterceptor` records call starts and durations for unary server calls. Add
`connect-rpc-scala-opentelemetry` and supply an OpenTelemetry SDK or `Meter` to its builder.
For configuration and metric names, see the [OpenTelemetry module README](opentelemetry/README.md).

Bind `metrics.andThen(logging)` to use both interceptors on a service. The runnable example also
uses `transformContext` on the client to send a request ID. [Interceptor example](examples/interceptors/README.md)

## http4s client

```scala
val result: Resource[IO, ConnectResponse[AddResponse]] =
  EmberClientBuilder.default[IO].build.evalMap { httpClient =>
    val transport = ConnectHttp4sClientBuilder(httpClient).build(uri"http://localhost:8080")
    val client = CalculatorServiceConnect.Client[IO](transport)
    client.add(AddRequest(20, 22), ConnectRequestContext())
  }
```

Call `.enableBinaryFormat()` on the client builder to use protobuf rather than JSON.

## Development

Run the unit, generator, integration, and dependency checks with:

```shell
sbt test
```

Run `make test-conformance-stable` to run the same unary Connect conformance checks as CI. The checks cover JSON and
binary protobuf on the http4s server over HTTP/1.1 and HTTP/2, and on the http4s client over HTTP/1.1.
