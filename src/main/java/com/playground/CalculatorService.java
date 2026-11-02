package com.playground;

import com.playground.grpc.BinaryOperation;
import com.playground.grpc.CalcResult;
import com.playground.grpc.CalculatorGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

/** Unary RPCs. Any BindableService bean is registered with the gRPC server automatically. */
@Service
public class CalculatorService extends CalculatorGrpc.CalculatorImplBase {

    @Override
    public void add(BinaryOperation request, StreamObserver<CalcResult> responseObserver) {
        respond(responseObserver, request.getLeft() + request.getRight());
    }

    @Override
    public void divide(BinaryOperation request, StreamObserver<CalcResult> responseObserver) {
        if (request.getRight() == 0) {
            // Errors travel as a gRPC Status, not as an exception thrown over the wire.
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Cannot divide by zero")
                    .asRuntimeException());
            return;
        }
        respond(responseObserver, request.getLeft() / request.getRight());
    }

    private void respond(StreamObserver<CalcResult> responseObserver, double value) {
        responseObserver.onNext(CalcResult.newBuilder().setValue(value).build());
        responseObserver.onCompleted();
    }
}
