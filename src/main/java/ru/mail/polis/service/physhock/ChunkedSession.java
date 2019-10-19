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

    private Iterator<Record> iterator;

    public ChunkedSession(Socket socket, HttpServer server) {
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

        final byte[] rn = "\r\n".getBytes(Charsets.UTF_8);
        final byte[] n = "\n".getBytes(Charsets.UTF_8);

        final int size = key.length + value.length + n.length;
        final byte[] hexSize = Integer.toHexString(size).getBytes(Charsets.UTF_8);
        final int len = size + hexSize.length + 2 * rn.length;

        final byte[] res = new byte[len];
        final ByteBuffer buffer = ByteBuffer.wrap(res);

        buffer.put(hexSize);
        buffer.put(rn);
        buffer.put(key);
        buffer.put(n);
        buffer.put(value);
        buffer.put(rn);

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

        final byte[] empty = "0\r\n\r\n".getBytes(Charsets.UTF_8);
        write(empty, 0, empty.length);

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
