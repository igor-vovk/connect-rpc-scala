# How to add Cross-Origin Resource Sharing (CORS) support

## The problem

You want to call your services from a web browser, but the browser blocks the request because of
the [same-origin policy](https://developer.mozilla.org/en-US/docs/Web/Security/Same-origin_policy)
(because the webpage is exposed on a different domain or port compared to your APIs).

## Solution

The problem can be solved by using existing CORS support in `http4s` server.
The only difference in the library would be to use `buildRoutes` method,
exposing routes that can be further manipulated, instead of `build` method.
The following example demonstrates how to add CORS support:

```scala 3
val httpServer: Resource[IO, org.http4s.server.Server] = {
  import com.comcast.ip4s.*
  import org.http4s.server.middleware.*

  for
    routes <- ConnectHttp4sRouteBuilder
      .forServices[IO](
        // Add your services here
      )
      // Using `buildRoutes` method instead of `build`
      .buildRoutes

    app = CORS.policy
      // Configure allowed origins
      .withAllowOriginHost(_.host.value == "localhost")
      .apply(routes.all)
      .orNotFound

    server <- EmberServerBuilder.default[IO]
      .withHost(host"0.0.0.0")
      .withPort(port"0")
      .withHttpApp(app)
      .build
  yield server
}

// Start the server
httpServer.useForever.unsafeRunSync()
```

## Remarks

In any case, it is better to expose your APIs on the same domain as your webpage, if possible.
CORS is always an additional request; separate domain or subdomain will also add additional DNS lookup time, both of
which can be avoided.
