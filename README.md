![](docs/connect-rpc-scala-logo.png)

# REST API for GRPC services with Connect protocol / GRPC Transcoding for Scala

![Maven Central](https://img.shields.io/maven-central/v/io.github.igor-vovk/connect-rpc-scala-core_3?style=flat-square&color=green)

This library makes it easy to expose your GRPC services to the clients using Connect protocol (with JSON messages),
without Envoy or any other proxy.

In essence, a service implementing the following protobuf definition:

```protobuf
syntax = "proto3";

package example;

service ExampleService {
  rpc GetExample(GetExampleRequest) returns (GetExampleResponse) {
  }
}

message GetExampleRequest {
  string id = 1;
}

message GetExampleResponse {
  string name = 1;
}
```

Will be exposed to the clients as a REST API:

```http
POST /example.ExampleService/GetExample HTTP/1.1
Content-Type: application/json

{
  "id": "123"
}

HTTP/1.1 200 OK

{
  "name": "example"
}
```

It is compatible with Connect protocol clients (e.g., you can generate clients with [Connect RPC](https://connectrpc.com) `protoc` and
`buf` plugins instead of writing requests manually).

In addition, the library supports creating free-form REST APIs,
using [GRPC Transcoding](https://cloud.google.com/endpoints/docs/grpc/transcoding) approach
(full support of `google.api.http` annotations is in progress).:

```protobuf
syntax = "proto3";

package example;

import "google/api/annotations.proto";

service ExampleService {
  rpc GetExample(GetExampleRequest) returns (GetExampleResponse) {
    option (google.api.http) = {
      get: "/example/{id}"
    };
  }
}

message GetExampleRequest {
  string id = 1;
}

message GetExampleResponse {
  string name = 1;
}
```

In addition to the previous way of calling it, this endpoint will be exposed as a REST API:

```http
GET /example/123 HTTP/1.1

HTTP/1.1 200 OK

{
  "name": "example"
}
```

Since integration happens on the foundational ScalaPB level, it works with all common GRPC code-generators:

* [ScalaPB](https://scalapb.github.io) services with `Future` monad
* [fs2-grpc](https://github.com/typelevel/fs2-grpc), built on top of `cats-effect` and `fs2`
* [ZIO gRPC](https://scalapb.github.io/zio-grpc/), built on top of `ZIO`

*Note*: at the moment, only unary (non-streaming) methods are supported.

## Usage

For SBT (you also need to install particular `http4s` server implementation):

```scala
libraryDependencies ++= Seq(
  "io.github.igor-vovk" %% "connect-rpc-scala-core" % "<version>",
  "org.http4s" %% "http4s-ember-server" % "0.23.29"
)
```

After installing the library, you can expose your GRPC service to the clients using Connect protocol (suppose you
already have a GRPC services generated with ScalaPB):

```scala
import org.ivovk.connect_rpc_scala.ConnectRpcHttpRoutes

// Your GRPC service(s)
val grpcServices: Seq[io.grpc.ServiceDefinition] = ???

val httpServer: Resource[IO, org.http4s.server.Server] = {
  import com.comcast.ip4s.*

  for {
    // Create httpApp with Connect-RPC routes, specifying your GRPC services
    httpApp <- ConnectRouteBuilder.forServices[IO](grpcServices).build

    // Create http server
    httpServer <- EmberServerBuilder.default[IO]
      .withHost(host"0.0.0.0")
      .withPort(port"8080")
      .withHttp2 // If you want to enable HTTP2 support
      .withHttpApp(httpApp)
      .build
  } yield httpServer
}

// Start the server
httpServer.use(_ => IO.never).unsafeRunSync()
```

### Hint: GRPC OpenTelemetry integration

Since the library creates a separate "fake" GRPC server, traffic going through it won't be captured by the
instrumentation of your main GRPC server (if any).

Here is how you can integrate OpenTelemetry with the Connect-RPC server:

```scala
val grpcServices: Seq[io.grpc.ServiceDefinition] = ??? // Your GRPC service(s)
val grpcOtel    : GrpcOpenTelemetry              = ??? // GrpcOpenTelemetry instance

ConnectRouteBuilder.forServices[IO](grpcServices)
  // Configure the server to use the same opentelemetry instance as the main server
  .withServerConfigurator { sb =>
    grpcOtel.configureServerBuilder(sb)
    sb
  }
  .build
```

### ZIO Interop

Because the library uses the Tagless Final pattern, it is perfectly possible to use it with ZIO. You might check
`zio_interop` branch, where conformance is implemented with `ZIO` and `ZIO-gRPC`.
You can read [this](https://zio.dev/guides/interop/with-cats-effect/).

## Development

### Connect RPC

#### Running Connect-RPC conformance tests

Run the following command to run Connect-RPC conformance tests:

```shell
docker build . --output "out" --progress=plain
```

Execution results are output to STDOUT.
Diagnostic data from the server itself is written to the log file `out/out.log`.

#### Connect protocol conformance tests status

✅ JSON codec conformance status: __full conformance__.

⌛ Protobuf codec conformance status: __13/72__ tests pass.

Known issues:

* Errors serialized incorrectly for protobuf codec.

#### Supported features of the protocol

```yaml
versions: [ HTTP_VERSION_1, HTTP_VERSION_2 ]
protocols: [ PROTOCOL_CONNECT ]
codecs: [ CODEC_JSON, CODEC_PROTO ]
stream_types: [ STREAM_TYPE_UNARY ]
supports_tls: false
supports_trailers: false
supports_connect_get: true
supports_message_receive_limit: false
```

### GRPC Transcoding

#### Supported features

- [x] GET, POST, PUT, DELETE, PATCH methods
- [x] Path parameters, e.g., `/v1/countries/{name}`
- [x] Query parameters, repeating query parameters (e.g., `?a=1&a=2`) as arrays
- [x] Request body (JSON)
- [x] Request body field mapping, e.g. `body: "request"` (supported), `body: "*"` (supported)
- [ ] Path suffixes, e.g., `/v1/{name=projects/*/locations/*}/datasets` (not supported yet)

### Future improvements

- [x] Support GET-requests ([#10](https://github.com/igor-vovk/connect-rpc-scala/issues/10))
- [x] Support `google.api.http` annotations (GRPC
  transcoding) ([#51](https://github.com/igor-vovk/connect-rpc-scala/issues/51))
- [ ] Support configurable timeouts
- [ ] Support non-unary (streaming) methods

### Thanks

The library is inspired and takes some ideas from the [grpc-json-bridge](https://github.com/avast/grpc-json-bridge).
Which doesn't seem to be supported anymore, + also the library doesn't follow a Connect-RPC standard (while being very
close to it).
