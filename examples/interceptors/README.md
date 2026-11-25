# Unary interceptors example

The generated Echo client uses `UnaryInterceptor.transformContext` to copy a typed request ID
attribute into a Connect header. The server composes OpenTelemetry metrics, error logging, and
an interceptor that stores the incoming request ID as a typed attribute. The handler reads it
with `context.attribute[RequestId]`.

Run from the repository root.

```sh
sbt 'example_interceptors / run'
```

The example uses Cedi to manage the meter provider and http4s routes. The in-process client sends
three calls, and the meter provider exports to the console.

- A successful echo returns the request ID received by the server.
- An empty request returns `INVALID_ARGUMENT` and appears in the metrics.
- A simulated outage returns `UNAVAILABLE`, appears in the metrics, and writes an `ERROR` log.

The OpenTelemetry SDK exports a start count of three and one duration measurement for each outcome
when the example exits.
