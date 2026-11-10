# Kubernetes health checks

Connect services can expose ordinary HTTP health endpoints through `google.api.http` transcoding. Kubernetes can use
those endpoints for its liveness and readiness probes without a separate gRPC health server or probe binary.

Define unary health methods with HTTP annotations:

```protobuf
syntax = "proto3";

package health.v1;

import "google/api/annotations.proto";

service HealthService {
  rpc Alive(AliveRequest) returns (AliveResponse) {
    option (google.api.http) = {get: "/v1/health"};
  }

  rpc Ready(ReadyRequest) returns (ReadyResponse) {
    option (google.api.http) = {get: "/v1/health/ready"};
  }
}

message AliveRequest {}
message AliveResponse {}
message ReadyRequest {}
message ReadyResponse {}
```

Implement and bind the generated service:

```scala
object HealthService extends HealthServiceConnect.Service[IO, ConnectRequestContext] {
  override def alive(
    request: AliveRequest,
    ctx: ConnectRequestContext,
  ): IO[ConnectResponse[AliveResponse]] =
    IO.pure(ConnectResponse(AliveResponse()))

  override def ready(
    request: ReadyRequest,
    ctx: ConnectRequestContext,
  ): IO[ConnectResponse[ReadyResponse]] =
    checkDependencies.as(ConnectResponse(ReadyResponse()))
}

val healthService = HealthServiceConnect.bind(HealthService)

val app = ConnectHttp4sRouteBuilder
  .forServices(applicationService, healthService)
  .build
```

The same http4s server now exposes `GET /v1/health` and `GET /v1/health/ready`. Point Kubernetes at those paths:

```yaml
containers:
  - name: application
    ports:
      - containerPort: 8080
        name: http
    readinessProbe:
      httpGet:
        port: http
        path: /v1/health/ready
    livenessProbe:
      httpGet:
        port: http
        path: /v1/health
```
