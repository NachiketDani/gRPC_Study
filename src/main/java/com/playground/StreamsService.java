package com.playground;

import com.playground.grpc.Average;
import com.playground.grpc.Number;
import com.playground.grpc.Prime;
import com.playground.grpc.PrimesRequest;
import com.playground.grpc.StreamsGrpc;
import com.playground.grpc.SumResult;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

/** The three streaming call types. */
@Service
public class StreamsService extends StreamsGrpc.StreamsImplBase {

    static final int MAX_PRIME_LIMIT = 1_000_000;

    /** Server streaming: send one Prime message per prime found. */
    @Override
    public void primes(PrimesRequest request, StreamObserver<Prime> responseObserver) {
        int limit = request.getLimit();
        if (limit < 0 || limit > MAX_PRIME_LIMIT) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("limit must be between 0 and " + MAX_PRIME_LIMIT)
                    .asRuntimeException());
            return;
        }
        var serverObserver = (ServerCallStreamObserver<Prime>) responseObserver;
        for (int candidate = 2; candidate <= limit; candidate++) {
            if (serverObserver.isCancelled()) {
                return; // the client went away, stop doing work
            }
            if (isPrime(candidate)) {
                serverObserver.onNext(Prime.newBuilder().setValue(candidate).build());
            }
        }
        serverObserver.onCompleted();
    }

    /** Client streaming: accumulate every number, answer once when the client finishes. */
    @Override
    public StreamObserver<Number> sum(StreamObserver<SumResult> responseObserver) {
        return new StreamObserver<>() {
            private int count;
            private double total;

            @Override
            public void onNext(Number number) {
                count++;
                total += number.getValue();
            }

            @Override
            public void onError(Throwable t) {
                // Client aborted; nothing to send back.
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(SumResult.newBuilder().setCount(count).setTotal(total).build());
                responseObserver.onCompleted();
            }
        };
    }

    /** Bidirectional streaming: reply to every number with the average so far. */
    @Override
    public StreamObserver<Number> runningAverage(StreamObserver<Average> responseObserver) {
        return new StreamObserver<>() {
            private int count;
            private double total;

            @Override
            public void onNext(Number number) {
                count++;
                total += number.getValue();
                responseObserver.onNext(Average.newBuilder().setCount(count).setAverage(total / count).build());
            }

            @Override
            public void onError(Throwable t) {
                // Client aborted; nothing to send back.
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    private static boolean isPrime(int n) {
        for (int i = 2; (long) i * i <= n; i++) {
            if (n % i == 0) {
                return false;
            }
        }
        return true;
    }
}
