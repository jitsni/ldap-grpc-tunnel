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
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;
import java.util.logging.Logger;

/*
 * @author Jitendra Kotamraju
 */
public class GrpcSSLSocketFactory extends SocketFactory {
    private static final Logger LOGGER = Logger.getLogger(GrpcSSLSocketFactory.class.getName());

    public static synchronized SocketFactory getDefault() {
        return new GrpcSSLSocketFactory();
    }

    @Override
    public Socket createSocket(String host, int port) {
        LOGGER.info(String.format("createSocket(%s, %d)\n", host, port));

        try {
            SSLContext sslContext = getSSLContext();
            SSLSocketFactory socketFactory = sslContext.getSocketFactory();
            LdapProxyGrpcServer ldapProxyGrpcServer = LdapProxyGrpcServerRegistry.getInstance().getTunnelEndpoint();
            GrpcSocket grpcSocket = new GrpcSocket(host, port, ldapProxyGrpcServer);
            return socketFactory.createSocket(grpcSocket, host, port, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    private SSLContext getSSLContext() throws Exception {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }

                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }

                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                }
        };

        SSLContext sc = SSLContext.getInstance("TLS");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());

        return sc;
    }
}
