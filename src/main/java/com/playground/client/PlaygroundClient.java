package com.playground.client;

import com.playground.grpc.Average;
import com.playground.grpc.BinaryOperation;
import com.playground.grpc.CalculatorGrpc;
import com.playground.grpc.Number;
import com.playground.grpc.Prime;
import com.playground.grpc.PrimesRequest;
import com.playground.grpc.StreamsGrpc;
import com.playground.grpc.SumResult;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Plain gRPC client (no Spring) that calls every RPC once and prints what happens.
 * Usage: PlaygroundClient [host:port]   (default localhost:9090)
 */
public final class PlaygroundClient {

    private static final long DEADLINE_SECONDS = 5;

    private final ManagedChannel channel;

    private PlaygroundClient(ManagedChannel channel) {
        this.channel = channel;
    }

    public static void main(String[] args) throws InterruptedException {
        String target = args.length > 0 ? args[0] : "localhost:9090";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        try {
            System.out.println("Calling " + target);
            PlaygroundClient client = new PlaygroundClient(channel);
            client.unary();
            client.unaryError();
            client.serverStreaming();
            client.clientStreaming();
            client.bidirectionalStreaming();
        } finally {
            channel.shutdown().awaitTermination(DEADLINE_SECONDS, TimeUnit.SECONDS);
        }
    }

    /** Unary: one request, one response. */
    private void unary() {
        var calculator = CalculatorGrpc.newBlockingStub(channel).withDeadlineAfter(DEADLINE_SECONDS, TimeUnit.SECONDS);
        var result = calculator.add(operation(2, 3));
        System.out.println("\n[unary] Add(2, 3) = " + result.getValue());
    }

    /** Errors surface as StatusRuntimeException carrying a gRPC status code. */
    private void unaryError() {
        var calculator = CalculatorGrpc.newBlockingStub(channel).withDeadlineAfter(DEADLINE_SECONDS, TimeUnit.SECONDS);
        try {
            calculator.divide(operation(1, 0));
        } catch (StatusRuntimeException e) {
            System.out.println("\n[unary error] Divide(1, 0) failed: " + e.getStatus().getCode()
                    + " - " + e.getStatus().getDescription());
        }
    }

    /** Server streaming: one request, the server sends many responses. */
    private void serverStreaming() {
        var streams = StreamsGrpc.newBlockingStub(channel).withDeadlineAfter(DEADLINE_SECONDS, TimeUnit.SECONDS);
        List<Integer> primes = new ArrayList<>();
        streams.primes(PrimesRequest.newBuilder().setLimit(30).build())
                .forEachRemaining((Prime prime) -> primes.add(prime.getValue()));
        System.out.println("\n[server streaming] Primes(limit=30) = " + primes);
    }

    /** Client streaming: the client sends many requests, the server answers once at the end. */
    private void clientStreaming() throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<Number> requests = StreamsGrpc.newStub(channel)
                .withDeadlineAfter(DEADLINE_SECONDS, TimeUnit.SECONDS)
                .sum(new PrintingObserver<SumResult>(done) {
                    @Override
                    public void onNext(SumResult result) {
                        System.out.println("\n[client streaming] Sum of 1..5 -> count=" + result.getCount()
                                + ", total=" + result.getTotal());
                    }
                });
        for (int i = 1; i <= 5; i++) {
            requests.onNext(number(i));
        }
        requests.onCompleted();
        awaitOrFail(done);
    }

    /** Bidirectional streaming: both sides stream at once; here an average comes back per number sent. */
    private void bidirectionalStreaming() throws InterruptedException {
        System.out.println("\n[bidirectional streaming] RunningAverage");
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<Number> requests = StreamsGrpc.newStub(channel)
                .withDeadlineAfter(DEADLINE_SECONDS, TimeUnit.SECONDS)
                .runningAverage(new PrintingObserver<Average>(done) {
                    @Override
                    public void onNext(Average average) {
                        System.out.println("  after " + average.getCount() + " number(s): average = "
                                + average.getAverage());
                    }
                });
        for (double value : new double[] {10, 20, 60}) {
            System.out.println("  sending " + value);
            requests.onNext(number(value));
        }
        requests.onCompleted();
        awaitOrFail(done);
    }

    private static void awaitOrFail(CountDownLatch done) throws InterruptedException {
        if (!done.await(DEADLINE_SECONDS, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting for the server to finish the stream");
        }
    }

    private static BinaryOperation operation(double left, double right) {
        return BinaryOperation.newBuilder().setLeft(left).setRight(right).build();
    }

    private static Number number(double value) {
        return Number.newBuilder().setValue(value).build();
    }

    /** Releases the latch when the stream ends, whether normally or with an error. */
    private abstract static class PrintingObserver<T> implements StreamObserver<T> {

        private final CountDownLatch done;

        PrintingObserver(CountDownLatch done) {
            this.done = done;
        }

        @Override
        public void onError(Throwable t) {
            System.out.println("  stream failed: " + t.getMessage());
            done.countDown();
        }

        @Override
        public void onCompleted() {
            done.countDown();
        }
    }
}
