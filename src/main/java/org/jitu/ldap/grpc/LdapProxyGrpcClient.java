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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.jitu.ldap.grpc.LdapService.Data;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.logging.Logger;

/*
 * @author Jitendra Kotamraju
 */
public class LdapProxyGrpcClient {
    private static final Logger LOGGER = Logger.getLogger(LdapProxyGrpcClient.class.getName());

    public void start() throws Exception {
        final CountDownLatch done = new CountDownLatch(1);

        // Create a channel and a stub
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50051)
                .usePlaintext()
                .build();
        TunnelGrpc.TunnelStub stub  = TunnelGrpc.newStub(channel);
        StreamObserver<LdapService.Session> responseObserver =
                new StreamObserver<LdapService.Session>() {

                    @Override
                    public void onNext(LdapService.Session session) {
                        try {
                            int sessionId = session.getTag();
                            LOGGER.info(String.format("LdapProxyGrpcClient <-- session = %d register ", sessionId));

                            String ldapUri = session.getTarget();
                            URI uri = new URI(ldapUri);
                            String host = uri.getHost();
                            int port = uri.getPort();
                            TcpProxy tcpProxy = new TcpProxy(sessionId, host, port);
                            LOGGER.info(String.format("Starting TCP proxy connection to LDAP server %s:%d", host, port));
                            tcpProxy.startConnection();
                            LOGGER.info(String.format("Started TCP proxy connection to LDAP server %s:%d", host, port));
                            tunnel(stub, sessionId, tcpProxy);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        t.printStackTrace();
                    }

                    @Override
                    public void onCompleted() {
                        LOGGER.info("All Done");
                    }
                };

        stub.register(LdapService.Session.newBuilder().build(), responseObserver);
    }

    void tunnel(TunnelGrpc.TunnelStub stub, int sessionId, TcpProxy tcpProxy) {
        StreamObserver<Data> responseObserver =
                new StreamObserver<Data>() {

                    @Override
                    public void onNext(Data value) {
                        LOGGER.info(String.format("LdapProxyGrpcClient <-- session : %d len = %d", sessionId, value.getData().size()));
                        byte[] data = value.getData().toByteArray();
                        try {
                            tcpProxy.write(data);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        t.printStackTrace();
                    }

                    @Override
                    public void onCompleted() {
                        LOGGER.info("All Done");
                    }
                };

        // Note: clientResponseObserver is handling both request and response stream processing.
        StreamObserver<Data> requestObserver = stub.tunnel(responseObserver);
        UpstreamWriter upstreamWriter = new UpstreamWriter(sessionId, requestObserver);
        tcpProxy.setReadConsumer(upstreamWriter);

        upstreamWriter.accept(new byte[0]);
    }

    private static class UpstreamWriter implements Consumer<byte[]> {
        private final int sessionId;
        private final StreamObserver<Data> requestObserver;

        UpstreamWriter(int sessionId, StreamObserver<Data> requestObserver) {
            this.sessionId = sessionId;
            this.requestObserver = requestObserver;
        }

        @Override
        public void accept(byte[] bytes) {
            Data data = Data.newBuilder().setTag(sessionId).setData(ByteString.copyFrom(bytes)).build();
            requestObserver.onNext(data);
        }
    }

}
