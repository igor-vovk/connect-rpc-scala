# Unary interceptors example

The generated Echo client uses `UnaryInterceptor.transformContext` to turn a request ID into a
Connect header. The server composes `OpenTelemetryServerInterceptor` and `ErrorLoggingInterceptor`
with `andThen` and passes the result to the generated service binder.

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
