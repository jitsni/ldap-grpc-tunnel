/**
 * Copyright 2022 Jitendra Kotamraju.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jitu.ldap.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.jitu.ldap.grpc.LdapService.Data;
import org.jitu.ldap.grpc.LdapService.Session;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Logger;

/*
 * @author Jitendra Kotamraju
 */
public class LdapProxyGrpcServer {
    private static final Logger LOGGER = Logger.getLogger(LdapProxyGrpcServer.class.getName());
    private static final String LDAP_GRPC_SERVER = "LdapGrpcServer";
    private static final String LDAP_GRPC_CLIENT = "LdapGrpcClient";

    volatile GrpcServerImpl grpcServer;
    private Server server;

    void start() throws IOException {
        grpcServer = new GrpcServerImpl();
        LdapProxyGrpcServerRegistry.getInstance().addTunnelEndpoint(this);

        /* The port on which the server should run */
        int port = 50052;
        server = ServerBuilder.forPort(port)
                .addService(grpcServer)
                .build()
                .start();
        LOGGER.info("LdapProxyGrpcServer started, listening on " + port);
    }

    static class GrpcServerImpl extends TunnelGrpc.TunnelImplBase {
        StreamObserver<Session> registerObserver;
        Map<Integer, CompletableFuture<MyStream>> streamMap = new ConcurrentHashMap<>();

        public void register(Session request, StreamObserver<Session> responseObserver) {
            LOGGER.info("GrpcServerImpl::register()");
            this.registerObserver = responseObserver;
        }

        @Override
        public StreamObserver<Data> tunnel(StreamObserver<Data> responseObserver) {
            LOGGER.info("GrpcServerImpl::tunnel()");

            MyStream stream = new MyStream(streamMap, responseObserver);

            return stream.getRquestObserver();
        }

        MyStream getStream(String host, int port, int sessionId)  {
            LOGGER.info(String.format("GrpcServerImpl::getStream(%d)", sessionId));
            CompletableFuture<MyStream> cf = new CompletableFuture<>();
            streamMap.put(sessionId, cf);
            String target = String.format("ldap://%s:%d", host, port);
            registerObserver.onNext(Session.newBuilder().setTarget(target).setTag(sessionId).build());
            try {
                return cf.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class MyStream implements Closeable {
        private final
        Map<Integer, CompletableFuture<MyStream>> streamMap;
        final TunnelWriteHandler writeHandler;
        Consumer<byte[]> readConsumer;
        StreamObserver<Data> responseObserver;
        int sessionId;

        MyStream(Map<Integer, CompletableFuture<MyStream>> streamMap, StreamObserver<Data> responseObserver) {
            this.streamMap = streamMap;
            this.responseObserver = responseObserver;

            writeHandler = new TunnelWriteHandler(sessionId, responseObserver);
        }

        void setReadConsumer(Consumer<byte[]> readConsumer) {
            this.readConsumer = readConsumer;
        }

        TunnelWriteHandler writeConsumer() {
            return writeHandler;
        }

        StreamObserver<Data> getRquestObserver() {
            return new StreamObserver<Data>() {
                @Override
                public void onNext(Data value) {
                    LOGGER.info(String.format("%s <-- %s session = %d  read = %d bytes", LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, value.getTag(), value.getData().size()));
                    sessionId = value.getTag();
                    CompletableFuture<MyStream> myStream = streamMap.get(sessionId);
                    if (myStream == null) {
                        throw new RuntimeException(String.format("%s <-- %s unknown session = %d", LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId));
                    }
                    if (!myStream.isDone()) {
                        myStream.complete(MyStream.this);
                    }
                    byte[] data = value.getData().toByteArray();
                    if (readConsumer == null) {
                        LOGGER.info(String.format("%s <-- %s session = %d readConsumer is null", LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId));
                    } else {
                        readConsumer.accept(data);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                }

                @Override
                public void onCompleted() {
                    LOGGER.info(String.format("%s <-- %s session = %d onCompleted", LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId));
                }
            };
        }

        @Override
        public void close() {
            LOGGER.info(String.format("%s --> %s session = %d onCompleted", LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId));
            responseObserver.onCompleted();
        }
    }

    // gRPC Server --> gRPC Client write handler
    static class TunnelWriteHandler implements Closeable {
        final int sessionId;
        final StreamObserver<Data> responseObserver;

        TunnelWriteHandler(int sessionId, StreamObserver<Data> responseObserver) {
            this.sessionId = sessionId;
            this.responseObserver = responseObserver;
        }

        public void write(byte[] bytes) {
            Data data = Data.newBuilder()
                    .setTag(sessionId)
                    .setData(ByteString.copyFrom(bytes))
                    .build();
            LOGGER.info(String.format("%s --> %s session = %d  write = %d bytes",
                    LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId, bytes.length));

            responseObserver.onNext(data);
        }

        @Override
        public void close() {
            LOGGER.info(String.format("%s --> %s session = %d onCompleted",
                    LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId));
            responseObserver.onCompleted();
        }
    }

}
