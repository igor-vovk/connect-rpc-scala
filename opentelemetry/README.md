# OpenTelemetry metrics

`connect-rpc-scala-opentelemetry` records OpenTelemetry metrics for unary Connect server calls.
Add the module to your application.

```scala
libraryDependencies += "me.ivovk" %% "connect-rpc-scala-opentelemetry" % "<version>"
```

Pass your configured `io.opentelemetry.api.OpenTelemetry` instance to the builder and bind the
interceptor to your service. This example uses the calculator service from the main README.
[Calculator service example](../README.md#define-and-implement-a-service)

```scala
import cats.effect.IO
import me.ivovk.connect_rpc_scala.interceptors.OpenTelemetryServerInterceptor
import me.ivovk.connect_rpc_scala.service.ConnectRequestContext

val metrics = OpenTelemetryServerInterceptor.builder
  .sdk(openTelemetry)
  .build[IO, ConnectRequestContext]()

val service = CalculatorServiceConnect.bind(Calculator, metrics)
```

Use `.meter(meter)` on the builder if your application already has a `Meter`. You can reuse the
interceptor across service bindings or compose it with `metrics.andThen(logging)`.
[RPC error logging](../README.md#rpc-error-logging)

| Metric | Instrument | Unit | Attributes |
| --- | --- | --- | --- |
| `connectrpc.server.call.started` | Counter | `{call}` | `connectrpc.method` |
| `connectrpc.server.call.duration` | Histogram | `s` | `connectrpc.method`, `connectrpc.status` |

The method attribute uses `package.Service/Method`. Duration uses the gRPC latency buckets and
measures the downstream interceptor chain and service handler. Calls enter the interceptor after
request decoding and leave before response encoding. Cancellation at this boundary, including when
the HTTP server cancels a handler because its deadline expired, is recorded as `CANCELLED`.
