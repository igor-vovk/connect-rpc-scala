# Define common variables
DOCKER_BUILD_CMD = docker build -f conformance-build/Dockerfile . --progress=plain --output out/

.PHONY: build
build-conformance:
	sbt conformance/stage

.PHONY: test-conformance
test-conformance: build-conformance
ifeq ($(profile),netty-server)
	@echo "Running conformance tests with profile: $(profile)"
	$(DOCKER_BUILD_CMD) --build-arg launcher=NettyServerLauncher --build-arg config=suite-netty.yaml --build-arg parallel_args="--parallel 1"
	@code=$$(cat out/exit_code | tr -d '\n'); echo "Exiting with code: $$code"; exit $$code
else ifeq ($(profile),http4s-server)
	@echo "Running conformance tests with profile: $(profile)"
	$(DOCKER_BUILD_CMD) --build-arg launcher=Http4sServerLauncher --build-arg config=suite-http4s.yaml --build-arg parallel_args="--parallel 1"
	@code=$$(cat out/exit_code | tr -d '\n'); echo "Exiting with code: $$code"; exit $$code
else ifeq ($(profile),http4s-server-nonstable)
	@echo "Running conformance tests with profile: $(profile)"
	$(DOCKER_BUILD_CMD) --build-arg launcher=Http4sServerLauncher --build-arg config=suite-http4s-nonstable.yaml --build-arg stable=false --build-arg parallel_args="--parallel 1"
	@code=$$(cat out/exit_code | tr -d '\n'); echo "Exiting with code: $$code"; exit $$code
else ifeq ($(profile),http4s-client)
	@echo "Running conformance tests with profile: $(profile)"
	$(DOCKER_BUILD_CMD) --build-arg launcher=Http4sClientLauncher --build-arg config=suite-http4s-client.yaml --build-arg mode=client
	@code=$$(cat out/exit_code | tr -d '\n'); echo "Exiting with code: $$code"; exit $$code
else
	@echo "Error: Unknown profile '$(profile)'. Supported profiles: netty-server, http4s-server, http4s-server-nonstable, http4s-client."
	@exit 1
endif
