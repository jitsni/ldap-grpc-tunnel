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

import java.io.Closeable;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * @author Jitendra Kotamraju
 */
public class LdapProxyGrpcClient {
    private static final Logger LOGGER = Logger.getLogger(LdapProxyGrpcClient.class.getName());
    private static final String LDAP_GRPC_SERVER = "LdapGrpcServer";
    private static final String LDAP_GRPC_CLIENT = "LdapGrpcClient";

    public void start() throws Exception {
        final CountDownLatch done = new CountDownLatch(1);

        // Create a channel and a stub
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50052)
                .usePlaintext()
                .build();
        TunnelGrpc.TunnelStub stub  = TunnelGrpc.newStub(channel);
        StreamObserver<LdapService.Session> responseObserver = new RegistrationHandler(stub);
        stub.register(LdapService.Session.newBuilder().build(), responseObserver);
    }

    private class RegistrationHandler implements StreamObserver<LdapService.Session> {

        private final TunnelGrpc.TunnelStub stub;

        RegistrationHandler(TunnelGrpc.TunnelStub stub) {
            this.stub = stub;
        }

        @Override
        public void onNext(LdapService.Session session) {
            try {
                int sessionId = session.getTag();
                LOGGER.info(String.format("%s --> %s session = %d register %s",
                        LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId, session));

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

    }

    void tunnel(TunnelGrpc.TunnelStub stub, int sessionId, TcpProxy tcpProxy) {
        TunnelReadHandler tunnelReadHandler = new TunnelReadHandler(sessionId, tcpProxy);

        StreamObserver<Data> requestObserver = stub.tunnel(tunnelReadHandler);
        tunnelReadHandler.setRequestObserver(requestObserver);
        TunnelWriteHandler tunnelWriteHandler = new TunnelWriteHandler(sessionId, requestObserver);
        tcpProxy.setReadHandler(tunnelWriteHandler);

        tunnelWriteHandler.handle(new byte[0], 0, 0, false, null);
    }

    // reads from gRPC tunnel and writes to TCP proxy
    class TunnelReadHandler implements StreamObserver<Data> {
        final int sessionId;
        final TcpProxy tcpProxy;
        StreamObserver<Data> requestObserver;
        volatile boolean done;

        TunnelReadHandler(int sessionId, TcpProxy tcpProxy) {
            this.sessionId = sessionId;
            this.tcpProxy = tcpProxy;
        }

        void setRequestObserver(StreamObserver<Data> requestObserver) {
            this.requestObserver = requestObserver;
        }

        @Override
        public void onNext(Data value) {
            LOGGER.info(String.format("%s --> %s session = %d read = %d bytes",
                    LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId, value.getData().size()));
            byte[] data = value.getData().toByteArray();
            try {
                tcpProxy.write(data);
            } catch (Exception e) {
                LOGGER.log(Level.INFO, String.format("%s --> %s session = %d onNext",
                        LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId), e);

                if (!done) {
                    done = true;
                    LOGGER.info(String.format("%s <-- %s session = %d onError",
                            LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId));
                    requestObserver.onError(e);
                }
                tcpProxy.close();
            }
        }

        @Override
        public void onError(Throwable t) {
            LOGGER.log(Level.INFO, String.format("%s --> %s session = %d onError",
                    LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId), t);

            if (!done) {
                done = true;
                LOGGER.info(String.format("%s <-- %s session = %d onError",
                        LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId));
                requestObserver.onError(t);
            }
            tcpProxy.close();
        }

        @Override
        public void onCompleted() {
            LOGGER.info(String.format("%s --> %s session = %d onCompleted",
                    LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId));

            if (!done) {
                done = true;
                LOGGER.info(String.format("%s <-- %s session = %d onCompleted",
                        LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId));
                requestObserver.onCompleted();
            }
            tcpProxy.close();
        }
    }

    // writes tcp proxy read data to gRPC tunnel
    private static class TunnelWriteHandler implements TcpProxy.LdapServerReadHandler, Closeable {
        private final int sessionId;
        private final StreamObserver<Data> requestObserver;
        volatile boolean done;

        TunnelWriteHandler(int sessionId, StreamObserver<Data> requestObserver) {
            this.sessionId = sessionId;
            this.requestObserver = requestObserver;
        }

        @Override
        public void handle(byte[] bytes, int off, int len, boolean close, Exception ex) {
            LdapService.Data.Builder builder = Data.newBuilder().setTag(sessionId);
            if (len == -1 || close || ex != null) {
                builder.setClose(close);
            } else {
                builder.setData(ByteString.copyFrom(bytes, off, len));
            }

            LOGGER.info(String.format("%s <-- %s session = %d write = %d bytes",
                    LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId, 0)); // TODO fix bytes
            requestObserver.onNext(builder.build());
        }

        @Override
        public void close() {
            if (!done) {
                done = true;
                LOGGER.info(String.format("%s <-- %s session = %d onCompleted",
                        LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId));
                requestObserver.onCompleted();
            }
        }

        public void error(Throwable t) {
            if (!done) {
                done = true;
                LOGGER.log(Level.INFO, String.format("%s <-- %s session = %d onError",
                        LDAP_GRPC_SERVER, LDAP_GRPC_CLIENT, sessionId), t);
                requestObserver.onError(t);
            }
        }
    }

}
