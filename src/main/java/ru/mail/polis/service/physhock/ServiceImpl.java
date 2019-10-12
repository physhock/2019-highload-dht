package ru.mail.polis.service.physhock;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.physhock.NoSuchElementExceptionLite;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServiceImpl extends HttpServer implements Service {

    private final DAO dao;
    private final ExecutorService reader;
    private final ExecutorService writer;

    public ServiceImpl(final int port, final DAO dao) throws IOException {
        super(getConfig(port), dao);
        this.dao = dao;
        this.reader = Executors.newFixedThreadPool(1);
        this.writer = Executors.newFixedThreadPool(4);
    }

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
    public Response getStatus(final Request request) {
        return request.getParameters().equals(Collections.EMPTY_LIST)
                ? new Response(Response.OK, "I am alive!\n".getBytes())
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
    public Response getData(@Param(value = "id", required = true) final String id) {
        try {
            return reader.submit(() -> {
                try {
                    geniusCheck(id);
                    ByteBuffer result = dao.get(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)));
                    ByteBuffer duplicate = result.duplicate();
                    byte[] body = new byte[duplicate.remaining()];
                    duplicate.get(body);
                    return new Response(Response.OK, body);
                } catch (NoSuchElementExceptionLite e) {
                    return new Response(Response.NOT_FOUND, Response.EMPTY);
                } catch (IOException e) {
                    return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
                } catch (IllegalArgumentException e) {
                    return new Response(Response.BAD_REQUEST, Response.EMPTY);
                }
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
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
    public Response putData(final Request request, @Param("id") final String id) {
        try {
            return writer.submit(() ->{
                try {
                    geniusCheck(id);
                    dao.upsert(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)), ByteBuffer.wrap(request.getBody()));
                    return new Response(Response.CREATED, Response.EMPTY);
                } catch (IOException e) {
                    return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
                } catch (IllegalArgumentException e) {
                    return new Response(Response.BAD_REQUEST, Response.EMPTY);
                }
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
    }

    /**
     * Method deletes data with defined id.
     *
     * @param id id
     * @return status of operation
     */
    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_DELETE)
    public Response deleteData(@Param("id") final String id) {
        try {
            geniusCheck(id);
            dao.remove(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)));
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        } catch (IllegalArgumentException e) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
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
}
