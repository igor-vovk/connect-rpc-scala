FROM alpine AS conformance
RUN apk add --no-cache bash curl tar

RUN mkdir /conformance
WORKDIR /conformance
RUN <<EOF
    CONFORMANCE_VERSION="v1.0.4"
    UNAME_OS=$(uname -s)
    UNAME_ARCH=$(uname -m)
    CONFORMANCE_URL="https://github.com/connectrpc/conformance/releases/download/${CONFORMANCE_VERSION}/connectconformance-${CONFORMANCE_VERSION}-${UNAME_OS}-${UNAME_ARCH}.tar.gz"
    echo "Downloading conformance from ${CONFORMANCE_URL}"
    curl -sSL "${CONFORMANCE_URL}" -o conformance.tgz
    tar -xvzf conformance.tgz
    chmod +x connectconformance
EOF

FROM sbtscala/scala-sbt:eclipse-temurin-23.0.1_11_1.10.5_3.3.4 AS build

ADD ./conformance /app/conformance
ADD ./core /app/core
ADD ./project /app/project
ADD ./build.sbt /app/build.sbt

WORKDIR /app
RUN sbt stage

FROM sbtscala/scala-sbt:eclipse-temurin-23.0.1_11_1.10.5_3.3.4 AS runner

COPY --from=conformance /conformance /conformance
ADD conformance-suite.yaml /conformance/
ADD conformance-suite-stable.yaml /conformance/
COPY --from=build /app/conformance/target/universal/stage /app
WORKDIR /conformance

RUN mkdir "/logs"

# Run stable tests first
RUN echo ">>>>> Running stable tests <<<<<"
RUN LOGS_PATH="/logs" \
    ./connectconformance \
    --conf conformance-suite-stable.yaml \
    --mode server \
    --parallel 1 \
    -v -vv --trace \
    -- \
    /app/bin/conformance

# Run unstable tests; allow them to fail
RUN echo ">>>>> Running unstable tests <<<<<"
RUN LOGS_PATH="/logs" \
    ./connectconformance \
    --conf conformance-suite.yaml \
    --mode server \
    --parallel 1 \
    -v -vv --trace \
    -- \
    /app/bin/conformance; \
    exit 0

FROM scratch AS export

COPY --link --from=runner /logs /

# connectconformance -h output:
#
# Runs conformance tests against a Connect implementation. Depending on the mode,
# the given command must be either a conformance client or a conformance server.
# When mode is both, two commands are given, separated by a quadruple-slash ("----"),
# with the client command being first and the server command second.
#
# A conformance client tests a client implementation: the command reads test cases
# from stdin. Each test case describes an RPC to make. The command then records
# the result of each operation to stdout. The input is a sequence of binary-encoded
# Protobuf messages of type connectrpc.conformance.v1.ClientCompatRequest,
# each prefixed with a fixed-32-bit length. The output is expected to be similar:
# a sequence of fixed-32-bit-length-prefixed messages, but the results are of
# type connectrpc.conformance.v1.ClientCompatResponse. The command should exit
# when it has read all test cases (i.e reached EOF of stdin) and then issued RPCs
# and recorded all results to stdout. The command should also exit and abort any
# in-progress RPCs if it receives a SIGTERM signal.
#
# A conformance server tests a server implementation: the command reads the required
# server properties from stdin. This comes in the form of a binary-encoded Protobuf
# message of type connectrpc.conformance.v1.ServerCompatRequest, prefixed with a
# fixed-32-bit length. The command should then start a server process and write its
# properties to stdout in the same form as the input, but using a
# connectrpc.conformance.v1.ServerCompatResponse message. The server process should
# provide an implementation of the test service defined by
# connectrpc.conformance.v1.ConformanceService. The command should exit
# upon receiving a SIGTERM signal. The command maybe invoked repeatedly, to start
# and test servers with different properties.
#
# A configuration file may be provided which specifies what features the client
# or server under test supports. This is used to filter the set of test cases
# that will be executed. If no config file is indicated, default configuration
# will be used.
#
# Flags can also be specified to filter the list of test case permutations run
# and change how results are interpreted. These are the --run, --skip,
# --known-failing, and --known-flaky flags. The --run and --skip flags should
# be used when running and troubleshooting specific test cases. For continuous
# integration tests, the --known-failing and --known-flaky flags should be used
# instead. With these, the tests are still run, but failing tests are interpreted
# differently. With --known-failing, the test cases must fail. This is useful to
# make sure that the list of known-failing test cases is updated if/when test
# failures are fixed. All of these flags support reading the list of test case
# patterns from a file using the "@" prefix. So a flag value with this prefix
# should be the path to a text file, which contains names or patterns, one per
# line.
#
# Usage:
#   connectconformance --mode [client|server] -- command...
#   connectconformance --mode both -- client-command... ---- server-command... [flags]
#
# Flags:
#       --bind string                 in client mode, the bind address on which the reference server should listen (0.0.0.0 means listen on all interfaces) (default "127.0.0.1")
#       --cert string                 in client mode, the path to a PEM-encoded TLS certificate file that the reference server should use
#       --conf string                 a config file in YAML format with supported features
#   -h, --help                        help for connectconformance
#       --key string                  in client mode, the path to a PEM-encoded TLS key file that the reference server should use
#       --known-failing stringArray   a pattern indicating the name of test cases that are known to fail; these test cases will be required to fail for the run to be successful; can be specified more than once
#       --known-flaky stringArray     a pattern indicating the name of test cases that are flaky; these test cases are allowed (but not required) to fail; can be specified more than once
#       --max-servers uint            the maximum number of server processes to be running in parallel (default 4)
#       --mode string                 required: the mode of the test to run; must be 'client', 'server', or 'both'
#   -p, --parallel uint               in server mode, the level of parallelism used when issuing RPCs (default 8)
#       --port uint                   in client mode, the port number on which the reference server should listen (implies --max-servers=1)
#       --run stringArray             a pattern indicating the name of test cases to run; when absent, all tests are run (other than indicated by --skip); can be specified more than once
#       --skip stringArray            a pattern indicating the name of test cases to skip; when absent, no tests are skipped; can be specified more than once
#       --test-file stringArray       a file in YAML format containing tests to run, which will skip running the embedded tests; can be specified more than once
#       --trace                       if true, full HTTP traces will be captured and shown alongside failing test cases
#   -v, --verbose                     enables verbose output
#       --version                     print version and exit
#       --vv                          enables even more verbose output