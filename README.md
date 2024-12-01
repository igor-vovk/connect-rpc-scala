![](docs/connect-rpc-scala-logo.png)

# Connect-RPC ↔ ScalaPB GRPC Bridge

This library provides a bridge between [Connect](https://connectrpc.com/docs/protocol) protocol and
[ScalaPB](https://scalapb.github.io) GRPC compiler for Scala.
It is inspired and takes ideas from [grpc-json-bridge](https://github.com/avast/grpc-json-bridge) library, which doesn't
seem to be supported anymore + the library doesn't follow a Connect-RPC standard (while being very close to it),
which makes using clients generated with ConnectRPC not possible.

Since integration happens on the foundational ScalaPB level, it works with all common GRPC code-generation projects for
Scala:

* [ScalaPB](https://scalapb.github.io) services with `Future` monad
* [fs2-grpc](https://github.com/typelevel/fs2-grpc), built on top of `cats-effect` and `fs2`
* [ZIO gRPC](https://scalapb.github.io/zio-grpc/), built on top of `ZIO`

*Note*: at the moment, only unary (non-streaming) methods are supported.

## Motivation

As a part of a GRPC adoption, there is usually a need to expose REST APIs.
Since GRPC is based on HTTP2, it's not the best option to expose it directly to the clients, which aren’t
natively supporting HTTP2.
So the most common approach is to expose REST APIs, which will be translated to GRPC on the server side.
There are two main protocols for this:

* [GRPC-WEB](https://github.com/grpc/grpc-web)
* [Connect](https://connectrpc.com/docs/introduction)

They are similar, but GRPC-WEB target is to be as close to GRPC as possible, while Connect is more
web-friendly: it has better client libraries, better web semantics:
content-type is `application/json` instead of `application/grpc-web+json`, error codes are just normal http codes
instead of being sent in headers, errors are output in the body of the response JSON-encoded, it supports GET-requests,
etc (you can also read
this [blog post describing why Connect is better](https://buf.build/blog/connect-a-better-grpc)).

Both protocols support encoding data in Protobuf and JSON.
JSON is more web-friendly, but it requires having some component in the middle, providing JSON → Protobuf
conversion during the request phase and Protobuf → JSON conversion during the response phase.

*And this can be tricky to set up*:

The suggested approach in this case is to use a web-server ([Envoy](https://scalapb.github.io)) as a proxy,
supporting translation of both protocols to GRPC.
The general setup of the Envoy in this case allows proxying HTTP/1.1 requests to GRPC, while still having protobuf
messages in the body of the request.

To support JSON, Envoy needs to be configured with Protobuf descriptors, which is not very convenient.

*That's where this library comes in*:

It allows exposing GRPC services, built with [ScalaPB](https://scalapb.github.io), to the clients
using Connect protocol (with JSON messages), without Envoy or any other proxy, so a web service can expose
both GRPC and REST APIs at the same time on two ports.

This simplifies overall setup: simpler CI, fewer network components, faster execution speed.

## Features of the protocol supported by the library

```yaml
versions: [ HTTP_VERSION_1 ]
protocols: [ PROTOCOL_CONNECT ]
codecs: [ CODEC_JSON ]
stream_types: [ STREAM_TYPE_UNARY ]
supports_tls: false
supports_trailers: false
supports_connect_get: true
supports_message_receive_limit: false
```

## Usage

Installing with SBT (you also need to install particular `http4s` server implementation):

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

### Running Connect-RPC conformance tests

Run the following command to run Connect-RPC conformance tests:

```shell
docker build . --output "out" --progress=plain
```

Execution results are output to STDOUT.
Diagnostic data from the server itself is written to the log file `out/out.log`.

### Conformance tests status

Current status: 11/79 tests pass.

Known issues:

* `google.protobuf.Any` serialization doesn't follow Connect-RPC
  spec: [#32](https://github.com/igor-vovk/connect-rpc-scala/issues/32)

## Future improvements

[x] Support GET-requests
[ ] Support non-unary (streaming) methods
