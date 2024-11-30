![](docs/connect-rpc-scala-logo.png)

# Connect-RPC ↔ ScalaPB GRPC Bridge

This library provides a bridge between [Connect-RPC](https://connectrpc.com/docs/protocol) protocol and
[ScalaPB](https://scalapb.github.io) GRPC compiler for Scala.
It is inspired and takes ideas from [grpc-json-bridge](https://github.com/avast/grpc-json-bridge) library, which doesn't
seem to be supported anymore + the library doesn't follow a Connect-RPC standard (while being very close to it),
which makes using clients generated with ConnectRPC not possible.

Since integration happens on the foundational ScalaPB level, it works with all common GRPC code-generation projects for
Scala:

* [ScalaPB](https://scalapb.github.io) services with `Future` monad
* [fs2-grpc](https://github.com/typelevel/fs2-grpc), built on top of `cats-effect` and `fs2`
* [ZIO gRPC](https://scalapb.github.io/zio-grpc/), built on top of `ZIO` monad (the most feature-rich implementation)

*Note*: at the moment, only unary (non-streaming) methods are supported.

## Motivation

As a part of a GRPC adoption, there is usually a need to expose REST APIs.
Since GRPC is based on HTTP2, it's not the best option to expose it directly to the clients, which aren’t
natively supporting HTTP2.
So the most common approach is to expose REST APIs, which will be translated to GRPC on the server side.
There are two main protocols for this:

* [GRPC-WEB](https://github.com/grpc/grpc-web)
* [Connect-RPC](https://connectrpc.com/docs/introduction)

They are similar, but GRPC-WEB target is to be as close to GRPC as possible, while Connect-RPC is more
web-friendly: it has better client libraries, better web semantics:
content-type is `application/json` instead of `application/grpc-web+json`, error codes are just normal http codes
instead of being sent in headers, errors are output in the body of the response JSON-encoded, it supports GET-requests,
etc (you can also read this [blog post describing why ConnectRPC is better](https://buf.build/blog/connect-a-better-grpc)).

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
using Connect-RPC protocol (with JSON messages), without Envoy or any other proxy, so a web service can expose
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

TODO: Add usage instructions

## Development

### Running Connect-RPC conformance tests

Run the following command to run Connect-RPC conformance tests:

```shell
docker build . --output "out" --progress=plain
```

Execution results are output to STDOUT.
Diagnostic data from the server itself is written to the log file `out/out.log`.

### Conformance tests status

Current status: 6/79 tests pass

Known issues:

* fs2-grpc server implementation doesn't support setting response headers
* `google.protobuf.Any` serialization doesn't follow Connect-RPC spec

## Future improvements

* Support GET-requests
* Support non-unary (streaming) methods
