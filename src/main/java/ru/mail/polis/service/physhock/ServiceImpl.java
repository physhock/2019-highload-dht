package ru.mail.polis.service.physhock;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
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
        return new Response(Response.OK, "I am alive!\n".getBytes());
    }

    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_GET)
    public Response getData(Request request, @Param("id") String id) {
        try {
            ByteBuffer result = dao.get(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)));
            ByteBuffer duplicate = result.duplicate();
            byte[] body = new byte[duplicate.remaining()];
            duplicate.get(body);
            return new Response(Response.OK, body);
        } catch (NoSuchElementException e) {
            return new Response(Response.NO_CONTENT, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_PUT)
    public Response putData(Request request, @Param("id") String id) {
        try {
            dao.upsert(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)), ByteBuffer.wrap(request.getBody()));
            return new Response(Response.CREATED, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_DELETE)
    public Response deleteData(Request request, @Param("id") String id) {
        try {
            dao.remove(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)));
            return new Response(Response.OK, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

}
