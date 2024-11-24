# Connect-RPC ↔ ScalaPB GRPC Bridge

This library provides a bridge between [Connect-RPC](https://connectrpc.com/docs/protocol) protocol and
[ScalaPB](https://scalapb.github.io) GRPC compiler for Scala.
It is inspired and takes ideas from [grpc-json-bridge](https://github.com/avast/grpc-json-bridge) library, which don't
seem to be supported anymore + the library doesn't follows a Connect-RPC standard (while being very close to it),
which makes using clients generated with ConnectRPC not possible.

At the moment, only unary (non-streaming) methods are supported.

## Motivation

As a part of a GRPC adoption, there is usually a need to expose REST APIs.
Since GRPC is based on HTTP2, it's not the best option to expose it directly to the clients, which aren’t
natively supporting HTTP2.
So the most common approach is to expose REST APIs, which will be translated to GRPC on the server side.
There are two main protocols for this:

* [GRPC-WEB](https://github.com/grpc/grpc-web)
* [Connect-RPC](https://connectrpc.com/docs/introduction)

They are similar, but GRPC-WEB target is to be as close to GRPC as possible, while Connect-RPC is more
web-friendly: better TypeScript libraries, better support for JSON, it supports GET-requests, etc.

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

## Usage

TODO: Add usage instructions

## Development

### Running Connect-RPC conformance tests

Run the following command to run Connect-RPC conformance tests:

```shell
docker build . --output "out" --progress=plain
```

Execution results are output in STDOUT.
Diagnostic data from the server itself is output in the `out/out.log` file.

### Conformance tests status

* Current status: 2/150 tests passed
* Not being able to set response headers from the fs2-grpc server implementation

## Future improvements

* Support GET-requests
* Support non-unary methods