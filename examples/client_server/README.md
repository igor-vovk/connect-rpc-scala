# Connect client-server example

This example generates a unary service API from protobuf, serves it through http4s, and calls it with the generated
Connect client.

Dependencies and resource allocation are wired using [Cedi](https://github.com/igor-vovk/cedi).

Run it from the repository root:

```shell
sbt example_client_server/run
```
