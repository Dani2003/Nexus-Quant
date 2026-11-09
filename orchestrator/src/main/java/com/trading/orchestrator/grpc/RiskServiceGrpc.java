package com.trading.orchestrator.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.AbstractAsyncStub;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractFutureStub;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * gRPC client stubs and service descriptors for risk.RiskService.
 */
public final class RiskServiceGrpc {

    private RiskServiceGrpc() {}

    public static final String SERVICE_NAME = "risk.RiskService";

    private static volatile MethodDescriptor<RiskRequest, RiskResponse> getEvaluateRiskMethod;

    public static MethodDescriptor<RiskRequest, RiskResponse> getEvaluateRiskMethod() {
        MethodDescriptor<RiskRequest, RiskResponse> method = getEvaluateRiskMethod;
        if (method == null) {
            synchronized (RiskServiceGrpc.class) {
                method = getEvaluateRiskMethod;
                if (method == null) {
                    getEvaluateRiskMethod = method = MethodDescriptor.<RiskRequest, RiskResponse>newBuilder()
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "EvaluateRisk"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(ProtoUtils.marshaller(RiskRequest.getDefaultInstance()))
                            .setResponseMarshaller(ProtoUtils.marshaller(RiskResponse.getDefaultInstance()))
                            .build();
                }
            }
        }
        return method;
    }

    public static RiskServiceStub newStub(Channel channel) {
        return RiskServiceStub.newStub(
                (c, o) -> new RiskServiceStub(c, o), channel);
    }

    public static RiskServiceBlockingStub newBlockingStub(Channel channel) {
        return RiskServiceBlockingStub.newStub(
                (c, o) -> new RiskServiceBlockingStub(c, o), channel);
    }

    public static RiskServiceFutureStub newFutureStub(Channel channel) {
        return RiskServiceFutureStub.newStub(
                (c, o) -> new RiskServiceFutureStub(c, o), channel);
    }

    public static final class RiskServiceStub extends AbstractAsyncStub<RiskServiceStub> {
        private RiskServiceStub(Channel channel, CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected RiskServiceStub build(Channel channel, CallOptions callOptions) {
            return new RiskServiceStub(channel, callOptions);
        }

        public void evaluateRisk(RiskRequest request, StreamObserver<RiskResponse> responseObserver) {
            ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getEvaluateRiskMethod(), getCallOptions()), request, responseObserver);
        }
    }

    public static final class RiskServiceBlockingStub extends AbstractBlockingStub<RiskServiceBlockingStub> {
        private RiskServiceBlockingStub(Channel channel, CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected RiskServiceBlockingStub build(Channel channel, CallOptions callOptions) {
            return new RiskServiceBlockingStub(channel, callOptions);
        }

        public RiskResponse evaluateRisk(RiskRequest request) {
            return ClientCalls.blockingUnaryCall(
                    getChannel(), getEvaluateRiskMethod(), getCallOptions(), request);
        }
    }

    public static final class RiskServiceFutureStub extends AbstractFutureStub<RiskServiceFutureStub> {
        private RiskServiceFutureStub(Channel channel, CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected RiskServiceFutureStub build(Channel channel, CallOptions callOptions) {
            return new RiskServiceFutureStub(channel, callOptions);
        }

        public ListenableFuture<RiskResponse> evaluateRisk(RiskRequest request) {
            return ClientCalls.futureUnaryCall(
                    getChannel().newCall(getEvaluateRiskMethod(), getCallOptions()), request);
        }
    }
}

