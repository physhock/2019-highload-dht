package ru.mail.polis.service.physhock;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.physhock.ByteBufferUtils;
import ru.mail.polis.dao.physhock.NoSuchElementExceptionLite;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.Executor;

public class ServiceImpl extends HttpServer implements Service {

    private final DAO dao;
    private final Executor executor;

    public ServiceImpl(final int port, final DAO dao, Executor executor) throws IOException {
        super(getConfig(port), dao);
        this.dao = dao;
        this.executor = executor;
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
        return Iterables.isEmpty(request.getParameters())
                ? new Response(Response.OK, "I am alive!\n".getBytes(Charset.defaultCharset()))
                : new Response(Response.BAD_REQUEST, Response.EMPTY);
    }

    /**
     * Method returns data if exists.
     *
     * @param id key value
     * @return data
     */
    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_GET)
    public void getData(@Param(value = "id", required = true) final String id, final HttpSession session) {
        executor.execute(() -> {
            try {
                geniusCheck(id);
                ByteBuffer result = dao.get(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)));
                final Response response = new Response(Response.OK, ByteBufferUtils.getByteArray(result));
                putResponseToSession(session, response);
            } catch (NoSuchElementExceptionLite e) {
                final Response response = new Response(Response.NOT_FOUND, Response.EMPTY);
                putResponseToSession(session, response);
            } catch (IOException e) {
                final Response response = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
                putResponseToSession(session, response);
            } catch (IllegalArgumentException e) {
                final Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
                putResponseToSession(session, response);
            }
        });
    }

    /**
     * Method puts data with defined id.
     *
     * @param request data
     * @param id      id
     * @return status of operation
     */
    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_PUT)
    public void putData(final Request request, @Param("id") final String id, final HttpSession session) {
        executor.execute(() -> {
            try {
                geniusCheck(id);
                dao.upsert(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)), ByteBuffer.wrap(request.getBody()));
                final Response response = new Response(Response.CREATED, Response.EMPTY);
                putResponseToSession(session, response);
            } catch (IOException e) {
                final Response response = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
                putResponseToSession(session, response);
            } catch (IllegalArgumentException e) {
                final Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
                putResponseToSession(session, response);
            }
        });
    }

    /**
     * Method deletes data with defined id.
     *
     * @param id id
     * @return status of operation
     */
    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_DELETE)
    public void deleteData(@Param("id") final String id, final HttpSession session) {
        executor.execute(() -> {
            try {
                geniusCheck(id);
                dao.remove(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)));
                final Response response = new Response(Response.ACCEPTED, Response.EMPTY);
                putResponseToSession(session, response);
            } catch (IOException e) {
                final Response response = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
                putResponseToSession(session, response);
            } catch (IllegalArgumentException e) {
                final Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
                putResponseToSession(session, response);
            }
        });

    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    private void geniusCheck(final String id) throws IllegalArgumentException {
        if ("".equals(id)) {
            throw new IllegalArgumentException("String ? return; // nothing to do");
        }
    }

    private void putResponseToSession(@NotNull final HttpSession session, final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            throw new UncheckedIOException("Session troubles", e);
        }
    }
}
