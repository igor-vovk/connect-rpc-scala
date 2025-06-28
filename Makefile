.PHONY: test-conformance

# Define common variables
DOCKER_BUILD_CMD = docker build -f build/conformance/Dockerfile . --progress=plain

test-conformance:
	@echo "Running conformance tests with profile: $(PROFILE)"
ifeq ($(PROFILE),netty-server)
	$(DOCKER_BUILD_CMD) --build-arg launcher=NettyServerLauncher --build-arg config=suite-netty.yaml
else ifeq ($(PROFILE),http4s-server)
	$(DOCKER_BUILD_CMD) --build-arg launcher=Http4sServerLauncher --build-arg config=suite-http4s.yaml
else ifeq ($(PROFILE),http4s-server-nonstable)
	$(DOCKER_BUILD_CMD) --build-arg launcher=Http4sServerLauncher --build-arg config=suite-http4s-nonstable.yaml --build-arg stable=false
else ifeq ($(PROFILE),http4s-client)
	$(DOCKER_BUILD_CMD) --build-arg launcher=Http4sClientLauncher --build-arg config=suite-http4s-client.yaml --build-arg mode=client
else
	@echo "Error: Unknown profile '$(PROFILE)'. Supported profiles: netty-server, http4s-server, http4s-server-nonstable, http4s-client."
	@exit 1
endif
