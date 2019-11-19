package ru.mail.polis.service.physhock;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import ru.mail.polis.Record;
import ru.mail.polis.dao.physhock.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class ChunkedSession extends HttpSession {

    private static final byte[] RN = "\r\n".getBytes(Charsets.UTF_8);
    private static final byte[] N = "\n".getBytes(Charsets.UTF_8);
    private static final byte[] EMPTY = "0\r\n\r\n".getBytes(Charsets.UTF_8);
    private Iterator<Record> iterator;

    public ChunkedSession(final Socket socket, final HttpServer server) {
        super(socket, server);
    }

    /**
     * Method takes the iterator as param and starts reading values
     * which iterator points to.
     *
     * @param iterator iterator
     * @throws IOException if smth goes wrong
     */
    public void stream(final Iterator<Record> iterator) throws IOException {
        this.iterator = iterator;

        final Response response = new Response(Response.OK);
        response.addHeader("Transfer-Encoding: chunked");
        writeResponse(response, false);

        next();
    }

    private byte[] makeChunk(final Record record) {
        final byte[] key = ByteBufferUtils.getByteArray(record.getKey());
        final byte[] value = ByteBufferUtils.getByteArray(record.getValue());

        final int size = key.length + value.length + N.length;
        final byte[] hexSize = Integer.toHexString(size).getBytes(Charsets.UTF_8);
        final int len = size + hexSize.length + 2 * RN.length;

        final byte[] res = new byte[len];
        final ByteBuffer buffer = ByteBuffer.wrap(res);

        buffer.put(hexSize);
        buffer.put(RN);
        buffer.put(key);
        buffer.put(N);
        buffer.put(value);
        buffer.put(RN);

        return res;
    }

    private void next() throws IOException {
        while (iterator.hasNext() && queueHead == null) {
            final Record record = iterator.next();
            final byte[] chunk = makeChunk(record);
            write(chunk, 0, chunk.length);
        }
        if (iterator.hasNext()) {
            return;
        }

        write(EMPTY, 0, EMPTY.length);

        server.incRequestsProcessed();
        if ((handling = pipeline.pollFirst()) != null) {
            if (handling == FIN) {
                scheduleClose();
            } else {
                server.handleRequest(handling, this);
            }
        }
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();
        next();
    }
}
