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

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.logging.Logger;

/*
 * @author Jitendra Kotamraju
 */
public class GrpcSocketFactory extends SocketFactory {
    private static final Logger LOGGER = Logger.getLogger(GrpcSocketFactory.class.getName());

    public static synchronized SocketFactory getDefault() {
        return new GrpcSocketFactory();
    }

    @Override
    public Socket createSocket(String ip, int port) throws IOException, UnknownHostException {
        LOGGER.info(String.format("createSocket(%s, %d)\n", ip, port));

        LdapProxyGrpcServer ldapProxyGrpcServer = LdapProxyGrpcServerRegistry.getInstance().getTunnelEndpoint();
        GrpcSocket grpcSocket = new GrpcSocket(ip, port, ldapProxyGrpcServer);
        return grpcSocket;
    }

    @Override
    public Socket createSocket(String ip, int port, InetAddress inetAddress, int i1) throws IOException, UnknownHostException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int port) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int port, InetAddress inetAddress1, int i1) throws IOException {
        throw new UnsupportedOperationException();
    }
}
