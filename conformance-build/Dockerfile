FROM docker.io/library/eclipse-temurin:25-jre AS base

FROM ghcr.io/igor-vovk/connect-conformance-dockerimage:1.0.5-1 AS conformance

FROM base AS runner

COPY --from=conformance /conformance /conformance
ADD conformance-build/*.yaml /conformance/
ADD conformance/target/universal/stage /app
WORKDIR /conformance

RUN mkdir "/out"

ARG config="suite.yaml"
ARG mode="server"
ARG launcher
ARG parallel_args=""

RUN LOGS_PATH="/out" \
    ./connectconformance \
    --conf $config \
    --mode $mode \
    $parallel_args \
    -v -vv --trace \
    -- \
    /app/bin/conformance -main org.ivovk.connect_rpc_scala.conformance.$launcher -Dcats.effect.warnOnNonMainThreadDetected=false; \
    echo $? > /out/exit_code

FROM scratch AS export

COPY --from=runner /out /
