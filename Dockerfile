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
ADD conformance-suite.yaml /conformance/conformance-suite.yaml
COPY --from=build /app/conformance/target/universal/stage /app
WORKDIR /conformance

RUN mkdir "/logs"
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

#13 0.151 Runs conformance tests against a Connect implementation. Depending on the mode,
#13 0.151 the given command must be either a conformance client or a conformance server.
#13 0.151 When mode is both, two commands are given, separated by a quadruple-slash ("----"),
#13 0.151 with the client command being first and the server command second.
#13 0.151
#13 0.151 A conformance client tests a client implementation: the command reads test cases
#13 0.151 from stdin. Each test case describes an RPC to make. The command then records
#13 0.151 the result of each operation to stdout. The input is a sequence of binary-encoded
#13 0.151 Protobuf messages of type connectrpc.conformance.v1.ClientCompatRequest,
#13 0.151 each prefixed with a fixed-32-bit length. The output is expected to be similar:
#13 0.151 a sequence of fixed-32-bit-length-prefixed messages, but the results are of
#13 0.151 type connectrpc.conformance.v1.ClientCompatResponse. The command should exit
#13 0.151 when it has read all test cases (i.e reached EOF of stdin) and then issued RPCs
#13 0.151 and recorded all results to stdout. The command should also exit and abort any
#13 0.151 in-progress RPCs if it receives a SIGTERM signal.
#13 0.151
#13 0.151 A conformance server tests a server implementation: the command reads the required
#13 0.151 server properties from stdin. This comes in the form of a binary-encoded Protobuf
#13 0.151 message of type connectrpc.conformance.v1.ServerCompatRequest, prefixed with a
#13 0.151 fixed-32-bit length. The command should then start a server process and write its
#13 0.151 properties to stdout in the same form as the input, but using a
#13 0.151 connectrpc.conformance.v1.ServerCompatResponse message. The server process should
#13 0.151 provide an implementation of the test service defined by
#13 0.151 connectrpc.conformance.v1.ConformanceService. The command should exit
#13 0.151 upon receiving a SIGTERM signal. The command maybe invoked repeatedly, to start
#13 0.151 and test servers with different properties.
#13 0.151
#13 0.151 A configuration file may be provided which specifies what features the client
#13 0.151 or server under test supports. This is used to filter the set of test cases
#13 0.151 that will be executed. If no config file is indicated, default configuration
#13 0.151 will be used.
#13 0.151
#13 0.151 Flags can also be specified to filter the list of test case permutations run
#13 0.151 and change how results are interpreted. These are the --run, --skip,
#13 0.151 --known-failing, and --known-flaky flags. The --run and --skip flags should
#13 0.151 be used when running and troubleshooting specific test cases. For continuous
#13 0.151 integration tests, the --known-failing and --known-flaky flags should be used
#13 0.151 instead. With these, the tests are still run, but failing tests are interpreted
#13 0.151 differently. With --known-failing, the test cases must fail. This is useful to
#13 0.151 make sure that the list of known-failing test cases is updated if/when test
#13 0.151 failures are fixed. All of these flags support reading the list of test case
#13 0.151 patterns from a file using the "@" prefix. So a flag value with this prefix
#13 0.151 should be the path to a text file, which contains names or patterns, one per
#13 0.151 line.
#13 0.152
#13 0.152 Usage:
#13 0.152   connectconformance --mode [client|server] -- command...
#13 0.152   connectconformance --mode both -- client-command... ---- server-command... [flags]
#13 0.152
#13 0.152 Flags:
#13 0.152       --bind string                 in client mode, the bind address on which the reference server should listen (0.0.0.0 means listen on all interfaces) (default "127.0.0.1")
#13 0.152       --cert string                 in client mode, the path to a PEM-encoded TLS certificate file that the reference server should use
#13 0.152       --conf string                 a config file in YAML format with supported features
#13 0.152   -h, --help                        help for connectconformance
#13 0.152       --key string                  in client mode, the path to a PEM-encoded TLS key file that the reference server should use
#13 0.152       --known-failing stringArray   a pattern indicating the name of test cases that are known to fail; these test cases will be required to fail for the run to be successful; can be specified more than once
#13 0.152       --known-flaky stringArray     a pattern indicating the name of test cases that are flaky; these test cases are allowed (but not required) to fail; can be specified more than once
#13 0.152       --max-servers uint            the maximum number of server processes to be running in parallel (default 4)
#13 0.152       --mode string                 required: the mode of the test to run; must be 'client', 'server', or 'both'
#13 0.152   -p, --parallel uint               in server mode, the level of parallelism used when issuing RPCs (default 8)
#13 0.152       --port uint                   in client mode, the port number on which the reference server should listen (implies --max-servers=1)
#13 0.152       --run stringArray             a pattern indicating the name of test cases to run; when absent, all tests are run (other than indicated by --skip); can be specified more than once
#13 0.152       --skip stringArray            a pattern indicating the name of test cases to skip; when absent, no tests are skipped; can be specified more than once
#13 0.152       --test-file stringArray       a file in YAML format containing tests to run, which will skip running the embedded tests; can be specified more than once
#13 0.152       --trace                       if true, full HTTP traces will be captured and shown alongside failing test cases
#13 0.152   -v, --verbose                     enables verbose output
#13 0.152       --version                     print version and exit
#13 0.152       --vv                          enables even more verbose output