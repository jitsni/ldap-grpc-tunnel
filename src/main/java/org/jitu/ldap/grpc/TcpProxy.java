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
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.logging.Logger;

/*
 * @author Jitendra Kotamraju
 */
public class TcpProxy implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(TcpProxy.class.getName());

    private final String ip;
    private final int port;
    private final int session;
    private Consumer<byte[]> readConsumer;

    Socket clientSocket;
    InputStream in;
    OutputStream out;

    TcpProxy(int session, String ip, int port) {
        this.session = session;
        this.ip = ip;
        this.port = port;
    }

    void setReadConsumer(Consumer<byte[]> readConsumer) {
        this.readConsumer = readConsumer;
    }

    public void startConnection() throws IOException {
        clientSocket = new Socket(ip, port);
        LOGGER.info(String.format("TcpProxy: socket = %s", clientSocket));

        out = clientSocket.getOutputStream();
        LOGGER.info(String.format("TcpProxy: session = %d output stream = %s", session, out));

        in = clientSocket.getInputStream();
        LOGGER.info(String.format("TcpProxy: session = %d input stream = %s", session, in));

        Reader reader = new Reader();
        new Thread(reader, "reader").start();
    }

    @Override
    public void close() throws IOException {
        clientSocket.close();
    }

    class Reader implements Runnable {

        public void run() {
            try {
                byte[] b = new byte[8192];

                while (true) {
                    int len = in.read(b);
                    LOGGER.info(String.format("TcpProxy: session = %d read() = %d bytes", session, len));
                    if (len == -1) {
                        break;
                    }
                    byte[] copy = Arrays.copyOf(b, len);

                    readConsumer.accept(copy);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    void write(byte[] data) throws Exception {
        LOGGER.info(String.format("TcpProxy:session = %d write() = %d bytes\n", session, data.length));
        out.write(data);
    }

}
