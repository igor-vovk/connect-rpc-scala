# gRPC and ConnectRPC Servers Example

This example demonstrates the pattern of starting both a gRPC server and a ConnectRPC server in the same component on
different ports.

The logic behind is that gRPC server is used for internal communication, while ConnectRPC server is proxied by a reverse
proxy to handle external http requests coming from the frontend.

Example shows that the same service is run by both servers, and the gRPC server is used to handle
gRPC requests, while the ConnectRPC server is used to handle ConnectRPC requests.

Dependencies are wired using [Cedi](https://github.com/igor-vovk/cedi) library.

Testing the ConnectRPC server:

1. Start the server by running:

```bash
sbt example_connectrpc_grpc_servers/run
```

2. Use `curl` to send http requests to the ConnectRPC server:

```bash
curl -X POST http://localhost:8080/examples.v1.ElizaService/Say -d '{"sentence": "Hello Connectrpc"}' -H 'Content-Type: application/json'
```
