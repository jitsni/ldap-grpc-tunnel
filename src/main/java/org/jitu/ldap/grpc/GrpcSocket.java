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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Logger;

/*
 * @author Jitendra Kotamraju
 */
public class GrpcSocket extends Socket {
    private static final Logger LOGGER = Logger.getLogger(GrpcSocket.class.getName());

    private static final AtomicInteger sessionId = new AtomicInteger(1);

    private final Consumer<byte[]> writeConsumer;
    private final LdapProxyGrpcServer.MyStream myStream;

    private int session;
    private MyInputStream in;
    private OutputStream out;

    GrpcSocket(String host, int port, LdapProxyGrpcServer ldapProxyGrpcServer) {
        this.session = sessionId.getAndIncrement();
        this.myStream = ldapProxyGrpcServer.grpcServer.getStream(host, port, session);
        this.writeConsumer = myStream.writeConsumer();
        in = new MyInputStream();
        myStream.setReadConsumer(in);
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if (in == null) {
            in = new MyInputStream();
        }
        return in;
    }

    @Override
    public void close() {
        LOGGER.info(String.format("session = %d close()", session));
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        if (out == null) {
            out = new MyOutputStream();
        }
        return out;
    }

    class MyOutputStream extends OutputStream {

        @Override
        public void write(int b) throws IOException {
            LOGGER.info(String.format("GrpcSocket: session = %d write()", session));

            byte[] copy = new byte[1];
            copy[0] = (byte) b;

            writeConsumer.accept(copy);
        }

        @Override
        public void write(byte[] b) throws IOException {
            LOGGER.info(String.format("GrpcSocket: session = %d write(b[%d])\n", session, b.length));

            byte[] copy = new byte[b.length];
            System.arraycopy(b, 0, copy, 0, b.length);

            writeConsumer.accept(copy);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            LOGGER.info(String.format("GrpcSocket: session = %d write(b[], %d, %d)\n", session, off, len));

            byte[] copy = new byte[len];
            System.arraycopy(b, off, copy, 0, len);

            writeConsumer.accept(copy);
        }

        @Override
        public void close() {
            LOGGER.info(String.format("GrpcSocket: session = %d output stream close()", session));
        }
    }

    class MyInputStream extends ByteArrayInputStream implements Consumer<byte[]> {

        BlockingDeque<byte[]> recvQueue;

        public MyInputStream() {
            super(new byte[0]);
            recvQueue = new LinkedBlockingDeque<>();
        }

        @Override
        public int read() {
            LOGGER.info(String.format("GrpcSocket: session = %d read()", session));
            if (available() == 0) {
                take();
            }
            return super.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            LOGGER.info(String.format("GrpcSocket: session = %d read(b[])", session));

            if (available() == 0) {
                take();
            }
            return super.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) {
            LOGGER.info(String.format("GrpcSocket: session = %d read(b, off, len)", session));

            if (available() == 0) {
                take();
            }
            int read = super.read(b, off, len);
            LOGGER.info(String.format("GrpcSocket: session = %d read(b, off, len) result = %d\n", session, read));

            return read;
        }

        @Override
        public void close() {
            LOGGER.info(String.format("GrpcSocket: session = %d input stream close()", session));
        }

        private void take() {
            try {
                LOGGER.info(String.format("GrpcSocket: session = %d take()", session));
                byte[] b = recvQueue.take();
                LOGGER.info(String.format("GrpcSocket: session = %d take() %d", session, b.length));

                int available = available();
                byte[] newbuf = new byte[available + b.length];
                System.arraycopy(buf, pos, newbuf, 0, available);
                System.arraycopy(b, 0, newbuf, available, b.length);

                buf = newbuf;
                pos = 0;
                count = newbuf.length;
                LOGGER.info(String.format("GrpcSocket: session = %d take() new buf pos=%d, count=%d\n", session, pos, count));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void accept(byte[] bytes) {
            recvQueue.offer(bytes);
        }
    }

}
