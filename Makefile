# Define common variables
DOCKER_BUILD_CMD = docker build -f conformance/build/Dockerfile . --progress=plain --output out/$@/
CHECK_CONFORMANCE_EXIT = code=$$(cat out/$@/exit_code) && echo "Exiting with code: $$code" && exit "$$code"

.PHONY: build-conformance
build-conformance:
	sbt conformance/stage

.PHONY: test-conformance-http4s-server test-conformance-http4s-server-proto
test-conformance-http4s-server: config := suite-http4s.yaml
test-conformance-http4s-server-proto: config := suite-http4s-server-proto.yaml
test-conformance-http4s-server test-conformance-http4s-server-proto: build-conformance
	@echo "Running conformance tests for http4s server with $(config)"
	$(DOCKER_BUILD_CMD) --build-arg launcher=Http4sServerLauncher \
						--build-arg config=$(config) \
						--build-arg parallel_args="--parallel 1"
	@$(CHECK_CONFORMANCE_EXIT)

.PHONY: test-conformance-http4s-server-nonstable
test-conformance-http4s-server-nonstable: test-conformance-http4s-server-proto

.PHONY: test-conformance-http4s-client test-conformance-http4s-client-proto
test-conformance-http4s-client: config := suite-http4s-client.yaml
test-conformance-http4s-client-proto: config := suite-http4s-client-proto.yaml
test-conformance-http4s-client test-conformance-http4s-client-proto: build-conformance
	@echo "Running conformance tests for http4s client with $(config)"
	$(DOCKER_BUILD_CMD) --build-arg launcher=Http4sClientLauncher \
						--build-arg config=$(config) \
						--build-arg mode=client
	@$(CHECK_CONFORMANCE_EXIT)

.PHONY: test-conformance-stable
test-conformance-stable: test-conformance-http4s-server \
						 test-conformance-http4s-client \
						 test-conformance-http4s-server-proto \
						 test-conformance-http4s-client-proto
