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
import java.util.NoSuchElementException;

public class ServiceImpl extends HttpServer implements Service {

    private final DAO dao;

    public ServiceImpl(int port, DAO dao) throws IOException {
        super(getConfig(port), dao);
        this.dao = dao;
    }

    private static HttpServerConfig getConfig(int port) {
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }

    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response getStatus(Request request) {
        return request.getParameters().equals(Collections.EMPTY_LIST) ?
                new Response(Response.OK, "I am alive!\n".getBytes()) : new Response(Response.BAD_REQUEST, Response.EMPTY);
    }

    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_GET)
    public Response getData(@Param(value = "id", required = true) String id) {
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
    }

    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_PUT)
    public Response putData(Request request, @Param("id") String id) {
        try {
            geniusCheck(id);
            dao.upsert(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)), ByteBuffer.wrap(request.getBody()));
            return new Response(Response.CREATED, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        } catch (IllegalArgumentException e) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
    }

    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_DELETE)
    public Response deleteData(@Param("id") String id) {
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
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    private void geniusCheck(String id) throws IllegalArgumentException {
        if (id.equals(""))
            throw new IllegalArgumentException("String ? return; // nothing to do");
    }
}
