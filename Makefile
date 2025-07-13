# Define common variables
DOCKER_BUILD_CMD = docker build -f conformance-build/Dockerfile . --progress=plain --output out/

.PHONY: build-conformance
build-conformance:
	sbt conformance/stage

.PHONY: test-conformance-http4s-server
test-conformance-http4s-server: build-conformance
	@echo "Running conformance tests for http4s server"
	$(DOCKER_BUILD_CMD) --build-arg launcher=Http4sServerLauncher --build-arg config=suite-http4s.yaml --build-arg parallel_args="--parallel 1"
	@code=$$(cat out/exit_code | tr -d '\n'); echo "Exiting with code: $$code"; exit $$code

.PHONY: test-conformance-http4s-server-nonstable
test-conformance-http4s-server-nonstable: build-conformance
	@echo "Running conformance tests for http4s server (non-stable)"
	$(DOCKER_BUILD_CMD) --build-arg launcher=Http4sServerLauncher --build-arg config=suite-http4s-nonstable.yaml --build-arg parallel_args="--parallel 1"
	@code=$$(cat out/exit_code | tr -d '\n'); echo "Exiting with code: $$code"; exit $$code

.PHONY: test-conformance-http4s-client
test-conformance-http4s-client: build-conformance
	@echo "Running conformance tests for http4s client"
	$(DOCKER_BUILD_CMD) --build-arg launcher=Http4sClientLauncher --build-arg config=suite-http4s-client.yaml --build-arg mode=client
	@code=$$(cat out/exit_code | tr -d '\n'); echo "Exiting with code: $$code"; exit $$code

.PHONY: test-conformance-netty-server
test-conformance-netty-server: build-conformance
	@echo "Running conformance tests for netty server"
	$(DOCKER_BUILD_CMD) --build-arg launcher=NettyServerLauncher --build-arg config=suite-netty.yaml --build-arg parallel_args="--parallel 1"
	@code=$$(cat out/exit_code | tr -d '\n'); echo "Exiting with code: $$code"; exit $$code

.PHONY: test-conformance-stable
test-conformance-stable: test-conformance-http4s-server \
						 test-conformance-http4s-client \
						 test-conformance-netty-server
