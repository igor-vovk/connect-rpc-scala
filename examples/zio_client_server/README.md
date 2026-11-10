# Connect ZIO client-server example

This example uses the generated request/response-only Connect service and client directly with ZIO `Task`.
`zio-interop-cats` provides the Cats Effect instances required by http4s, so the application does not need a `Future`,
a gRPC channel, or zio-grpc.

Run it from the repository root:

```shell
sbt example_zio_client_server/run
```
