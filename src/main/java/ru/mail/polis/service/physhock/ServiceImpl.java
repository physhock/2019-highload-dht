package ru.mail.polis.service.physhock;

import com.google.common.base.Charsets;
import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.physhock.ByteBufferUtils;
import ru.mail.polis.dao.physhock.NoSuchElementExceptionLite;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.concurrent.Executor;

/**
 * Implementation of Service.
 */
public class ServiceImpl extends HttpServer implements Service {

    private static final Response INTERNAL_ERROR = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
    private static final Response BAD_REQUEST = new Response(Response.BAD_REQUEST, Response.EMPTY);
    private final DAO dao;
    private final Executor executor;
    private final Topology<String> topology;
    private final HttpClient httpClient;


    /**
     * Server constructor.
     *
     * @param port     server port
     * @param dao      dao ( RocksDB dao)
     * @param executor executor service
     * @throws IOException super thrower
     */
    public ServiceImpl(final int port, final DAO dao,
                       final Executor executor,
                       final Topology<String> topology) throws IOException {
        super(getConfig(port), dao);
        this.dao = dao;
        this.executor = executor;
        this.topology = topology;
        httpClient = new ;
    }

    @NotNull
    private static HttpServerConfig getConfig(final int port) {
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }

    /**
     * Method returns current server status.
     *
     * @param request http request
     * @return current status
     */
    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response getStatus(@NotNull final Request request) {
        return new Response(Response.OK, "I am alive!\n".getBytes(Charset.defaultCharset()));
    }

    /**
     * Method returns data if exists.
     *
     * @param id key value
     */
    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_GET)
    public void getData(@Param(value = "id", required = true) final String id, final HttpSession session) {
        if(geniusCheck(id, session)) {
            executor.execute(() -> {
                try {
                    final ByteBuffer result = dao.get(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)));
                    final Response response = new Response(Response.OK, ByteBufferUtils.getByteArray(result));
                    putResponseToSession(session, response);
                } catch (NoSuchElementExceptionLite e) {
                    final Response response = new Response(Response.NOT_FOUND, Response.EMPTY);
                    putResponseToSession(session, response);
                } catch (IOException e) {
                    putResponseToSession(session, INTERNAL_ERROR);
                }
            });
        }
    }

    /**
     * Method puts data with defined id.
     *
     * @param request data
     * @param id      id
     */
    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_PUT)
    public void putData(final Request request, @Param("id") final String id, final HttpSession session) {
        if(geniusCheck(id, session)) {
            executor.execute(() -> {
                try {
                    dao.upsert(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)), ByteBuffer.wrap(request.getBody()));
                    final Response response = new Response(Response.CREATED, Response.EMPTY);
                    putResponseToSession(session, response);
                } catch (IOException e) {
                    putResponseToSession(session, INTERNAL_ERROR);
                }
            });
        }
    }

    /**
     * Method deletes data with defined id.
     *
     * @param id id
     */
    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_DELETE)
    public void deleteData(@Param("id") final String id, final HttpSession session) {
        if(geniusCheck(id, session)) {
            executor.execute(() -> {
                try {
                    dao.remove(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)));
                    final Response response = new Response(Response.ACCEPTED, Response.EMPTY);
                    putResponseToSession(session, response);
                } catch (IOException e) {
                    putResponseToSession(session, INTERNAL_ERROR);
                }
            });
        }
    }

    /**
     * Method get data in requested range.
     *
     * @param start   start form
     * @param end     end by
     * @param session http session which wil
     */
    @Path("/v0/entities")
    @RequestMethod(Request.METHOD_GET)
    public void getRange(@Param(value = "start", required = true) final String start,
                         @Param(value = "end") final String end,
                         final HttpSession session) {
        if (geniusCheck(start, session)) {
            executor.execute(() -> {
                final ByteBuffer from = ByteBuffer.wrap(start.getBytes(Charsets.UTF_8));
                final ByteBuffer to = end == null || end.isEmpty()
                        ? null
                        : ByteBuffer.wrap(end.getBytes(Charsets.UTF_8));
                try {
                    final Iterator<Record> iterator = dao.range(from, to);
                    final ChunkedSession storageSession = (ChunkedSession) session;
                    storageSession.stream(iterator);
                } catch (IOException e) {
                    throw new UncheckedIOException("Session troubles", e);
                }
            });
        }
    }

    @Override
    public void handleDefault(final Request request, @NotNull final HttpSession session) throws IOException {
        session.sendResponse(BAD_REQUEST);
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new ChunkedSession(socket, this);
    }

    private boolean geniusCheck(final String id, final HttpSession session) {
        final boolean equals = "".equals(id);
        if (equals) {
            putResponseToSession(session, BAD_REQUEST);
        }
        return !equals;
    }

    private void putResponseToSession(@NotNull final HttpSession session, final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            throw new UncheckedIOException("Session troubles", e);
        }
    }

    private boolean nodeCoordinator(ByteBuffer key, Request request){

        final String node = topology.calculateFor(key);
        if (topology.isMe(node))
            return true;
        else
    }
}
