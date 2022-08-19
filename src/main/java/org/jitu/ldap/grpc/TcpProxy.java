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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * @author Jitendra Kotamraju
 */
class TcpProxy implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(TcpProxy.class.getName());
    private static final String TCP_PROXY = "TcpProxy";
    private static final String LDAP = "LdapServer";
    private static final AtomicInteger THREAD_NUMBER = new AtomicInteger(1);

    private final String host;
    private final int port;
    private final int session;
    private LdapServerReadHandler readHandler;

    private Socket socket;
    private InputStream in;
    private OutputStream out;
    private volatile boolean closed;

    TcpProxy(int session, String host, int port) {
        this.session = session;
        this.host = host;
        this.port = port;
    }

    void setReadHandler(LdapServerReadHandler readHandler) {
        this.readHandler = readHandler;
    }

    void startConnection() throws IOException {
        socket = new Socket(host, port);
        LOGGER.info(String.format("session = %d socket = %s", session, socket));

        out = socket.getOutputStream();
        LOGGER.info(String.format("session = %d output stream = %s", session, out));

        in = socket.getInputStream();
        LOGGER.info(String.format("session = %d input stream = %s", session, in));

        LdapServerReader reader = new LdapServerReader();
        String name = "tcp-proxy-" + THREAD_NUMBER.getAndIncrement();
        Thread thread = new Thread(reader, name);
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                if (socket != null) {
                    LOGGER.info(String.format("%s socket close: session = %d", TCP_PROXY, session));
                    socket.close();
                }
            } catch (IOException ioe) {
                // ignore
            }
        }
    }

    private class LdapServerReader implements Runnable {

        public void run() {
            try {
                byte[] b = new byte[8192];

                while (true) {
                    int len = in.read(b);
                    LOGGER.info(String.format("%s <-- %s: session = %d read = %d bytes", TCP_PROXY, LDAP, session, len));
                    if (len == -1) {
                        readHandler.handle(b, 0, len, true, null);
                        break;
                    }
                    readHandler.handle(b, 0, len, false, null);
                }
            } catch (Exception e) {
                if (!closed) {
                    LOGGER.log(Level.INFO, "Error reading ldap server", e);
                    readHandler.handle(null, 0, 0, false, e);
                }
            } finally {
                close();
            }
        }

    }

    void write(byte[] data) throws Exception {
        LOGGER.info(String.format("%s --> %s: session = %d write = %d bytes\n", TCP_PROXY, LDAP, session, data.length));
        out.write(data);
    }

    interface LdapServerReadHandler {
        void handle(byte[] bytes, int off, int len, boolean close, Exception ex);
    }

}
