# Implementing Kubernetes health checks

## The problem

Kubernetes support for GRPC health checks is quite limited.
Only [liveness GRPC health probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-grpc-liveness-probe)
are supported out of the box (the pod is killed when it fails, which is not what you want in most cases).
The usual way to overcome it involves
installing [grpc-health-probe](https://github.com/grpc-ecosystem/grpc-health-probe) utility to the container,
which is also not optimal.

This document describes how to implement readiness health checks for GRPC services.

## The solution

You describe your health check logic as a GRPC service and expose it as an HTTP route.
Then, you can use
Kubernetes [HTTP liveness/readiness](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-http-request)
probes to check the health of your service.

An additional benefit of creating custom health checks is that the default `HealthStatusManager` Java implementation of
GRPC health checks uses a push model to update state of the health checks, which is harder to implement
(requires having some built-in scheduler to periodically check the health of the services and update
`HealthStatusManager` state, leading to a question how you will monitor the scheduler if it will start failing updating
health state).

In your own health check service, you can use a pull model, where the health of the underlying services is checked only
when the health check endpoint is called.

### Step 1: Define your own health check service

```protobuf
// health.proto
syntax = "proto3";

package health.v1;

service HealthService {
  rpc Alive(AliveRequest) returns (AliveResponse) {
    option (google.api.http) = {get: "/v1/health"};
  }

  rpc Ready(ReadyRequest) returns (ReadyResponse) {
    option (google.api.http) = {get: "/v1/health/ready"};
  }
}

message AliveRequest {}

message AliveResponse {
  repeated string services = 1;
}

message ReadyRequest {}

message ReadyResponse {
  repeated string services = 1;
}
```

Using `google.api.http` annotations, you can expose your health check service as user-defined HTTP route.
In this example, the `Alive` method is exposed as `/v1/health` and the `Ready` method is exposed as `/v1/health/ready`.

### Step 2: Implement health check service

Here you can implement your health check logic.
You can leave alive check empty so it will be used only to check connectivity with the web-server,
and implement all your underlying health-checks in the ready endpoint.

### Step 3: Expose the health check service as an HTTP route

In this case list your health check service when instantiating `ConnectHttp4sRouteBuilder`:

```scala
val httpHealthGrpcServiceDef = ???

ConnectHttp4sRouteBuilder
  .forServices[IO](
    // other services
    httpHealthGrpcServiceDef,
  )
  .build
```

### Step 4:

Tune your Kubernetes deployment to use the health check service:

```yaml
apiVersion: apps/v1
kind: Deployment

metadata:
  name: your-container

spec:
  selector:
    matchLabels:
      name: your-container
  template:
    metadata:
      labels:
        name: your-container
    spec:
      containers:
        - name: your-container
          image: https://your-registry/your-image:latest
          ports:
            - containerPort: 9090
              name: grpc
            - containerPort: 8080
              name: http
          readinessProbe:
            httpGet:
              port: http
              path: /v1/health/ready
            initialDelaySeconds: 5
          livenessProbe:
            httpGet:
              port: http
              path: /v1/health
            initialDelaySeconds: 5
```

### Step 5: Observe the health of your service

Now you can observe the health of your service using Kubernetes probes.
