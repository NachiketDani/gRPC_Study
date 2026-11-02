# gRPC Playground

A small Spring Boot app for learning gRPC. It exposes all four gRPC call types on port 9090.

| Service | RPC | Call type | What it does |
|---|---|---|---|
| `Calculator` | `Add` | Unary | `left + right` |
| `Calculator` | `Divide` | Unary | `left / right`; fails with `INVALID_ARGUMENT` when `right` is 0 |
| `Streams` | `Primes` | Server streaming | Streams every prime up to `limit` (max 1,000,000) |
| `Streams` | `Sum` | Client streaming | Client sends numbers, server replies once with count and total |
| `Streams` | `RunningAverage` | Bidirectional streaming | Server replies with the running average after each number |

Stack: Java 21, Spring Boot 4.1 (native `spring-boot-starter-grpc-server`), Maven.

## Where to look

- `src/main/proto/playground.proto`: the contract. Java stubs are generated from it at build time into `target/generated-sources`.
- `CalculatorService`, `StreamsService`: implementations. Any `BindableService` bean is registered with the server automatically.
- `PlaygroundGrpcTests`: calls every RPC through a real gRPC channel using the in-process transport (no ports needed).

## Run

```bash
mvn test                  # run the tests
mvn spring-boot:run       # start the server on :9090
```

With the server running, call every RPC once from the command line client (a plain gRPC client, no Spring, in `PlaygroundClient`). Run it from a second terminal:

```bash
mvn -q compile exec:java
mvn -q compile exec:java -Dexec.args="otherhost:9090"   # different target
```

Reflection and the standard health service are enabled, so tools like [grpcurl](https://github.com/fullstorydev/grpcurl) work without the `.proto` file:

```bash
grpcurl -plaintext localhost:9090 list
grpcurl -plaintext -d '{"left": 10, "right": 4}' localhost:9090 playground.v1.Calculator/Divide
grpcurl -plaintext -d '{"left": 1, "right": 0}'  localhost:9090 playground.v1.Calculator/Divide
grpcurl -plaintext -d '{"limit": 30}'            localhost:9090 playground.v1.Streams/Primes
```
