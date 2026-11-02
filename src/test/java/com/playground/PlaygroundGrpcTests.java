package com.playground;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.playground.grpc.Average;
import com.playground.grpc.BinaryOperation;
import com.playground.grpc.CalculatorGrpc;
import com.playground.grpc.Number;
import com.playground.grpc.Prime;
import com.playground.grpc.PrimesRequest;
import com.playground.grpc.StreamsGrpc;
import com.playground.grpc.SumResult;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.grpc.test.autoconfigure.AutoConfigureTestGrpcTransport;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.grpc.client.GrpcChannelFactory;

/** Exercises every RPC through a real gRPC channel; the in-process transport avoids opening ports. */
@SpringBootTest
@AutoConfigureTestGrpcTransport
class PlaygroundGrpcTests {

    private static final long TIMEOUT_SECONDS = 5;

    @Autowired
    private GrpcChannelFactory channels;

    private ManagedChannel channel;

    @BeforeEach
    void openChannel() {
        channel = channels.createChannel("playground");
    }

    @AfterEach
    void closeChannel() {
        channel.shutdownNow();
    }

    // ---- Unary ----

    @Test
    void addReturnsSum() {
        var result = CalculatorGrpc.newBlockingStub(channel).add(operation(2, 3));

        assertThat(result.getValue()).isEqualTo(5.0);
    }

    @Test
    void divideReturnsQuotient() {
        var result = CalculatorGrpc.newBlockingStub(channel).divide(operation(10, 4));

        assertThat(result.getValue()).isEqualTo(2.5);
    }

    @Test
    void divideByZeroFailsWithInvalidArgument() {
        var calculator = CalculatorGrpc.newBlockingStub(channel);

        assertThatThrownBy(() -> calculator.divide(operation(1, 0)))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription()).contains("divide by zero");
                });
    }

    // ---- Server streaming ----

    @Test
    void primesStreamsEveryPrimeUpToTheLimit() {
        Iterator<Prime> primes = StreamsGrpc.newBlockingStub(channel)
                .primes(PrimesRequest.newBuilder().setLimit(30).build());

        List<Integer> received = new ArrayList<>();
        primes.forEachRemaining(prime -> received.add(prime.getValue()));

        assertThat(received).containsExactly(2, 3, 5, 7, 11, 13, 17, 19, 23, 29);
    }

    @Test
    void primesRejectsLimitAboveMaximum() {
        Iterator<Prime> primes = StreamsGrpc.newBlockingStub(channel)
                .primes(PrimesRequest.newBuilder().setLimit(StreamsService.MAX_PRIME_LIMIT + 1).build());

        assertThatThrownBy(primes::hasNext)
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    // ---- Client streaming ----

    @Test
    void sumAccumulatesEveryNumberTheClientSends() throws InterruptedException {
        BlockingQueue<SumResult> results = new LinkedBlockingQueue<>();
        StreamObserver<Number> requests = StreamsGrpc.newStub(channel).sum(collectInto(results));

        for (int i = 1; i <= 4; i++) {
            requests.onNext(number(i));
        }
        requests.onCompleted();

        SumResult result = results.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(result).isNotNull();
        assertThat(result.getCount()).isEqualTo(4);
        assertThat(result.getTotal()).isEqualTo(10.0);
    }

    // ---- Bidirectional streaming ----

    @Test
    void runningAverageRepliesToEveryNumber() throws InterruptedException {
        BlockingQueue<Average> averages = new LinkedBlockingQueue<>();
        StreamObserver<Number> requests = StreamsGrpc.newStub(channel).runningAverage(collectInto(averages));

        requests.onNext(number(2));
        requests.onNext(number(4));
        requests.onNext(number(6));
        requests.onCompleted();

        List<Double> received = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Average average = averages.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(average).as("average #%d", i + 1).isNotNull();
            received.add(average.getAverage());
        }
        assertThat(received).containsExactly(2.0, 3.0, 4.0);
    }

    // ---- helpers ----

    private static BinaryOperation operation(double left, double right) {
        return BinaryOperation.newBuilder().setLeft(left).setRight(right).build();
    }

    private static Number number(double value) {
        return Number.newBuilder().setValue(value).build();
    }

    private static <T> StreamObserver<T> collectInto(BlockingQueue<T> queue) {
        return new StreamObserver<>() {
            @Override
            public void onNext(T value) {
                queue.add(value);
            }

            @Override
            public void onError(Throwable t) {
                throw new AssertionError("unexpected gRPC error", t);
            }

            @Override
            public void onCompleted() {
                // nothing to do; results are already queued
            }
        };
    }
}
