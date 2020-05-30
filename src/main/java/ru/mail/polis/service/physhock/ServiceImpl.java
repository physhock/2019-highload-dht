package ru.mail.polis.service.physhock;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.physhock.ByteBufferUtils;
import ru.mail.polis.dao.physhock.NoSuchElementExceptionLite;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.physhock.util.Converter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Implementation of Service.
 */
public class ServiceImpl extends HttpServer implements Service {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final Response BAD_REQUEST = new Response(Response.BAD_REQUEST, Response.EMPTY);
    private static final String PROXIED_REQUEST = "Request-from-node";
    private final DAO dao;
    private final Executor executor;
    private final Topology<String> topology;
    private final Map<String, HttpClient> nodes;

    /**
     * Server constructor.
     *
     * @param port     server port
     * @param dao      dao ( RocksDB dao)
     * @param executor executor service
     * @throws IOException internal error
     */
    public ServiceImpl(final int port, final DAO dao,
                       final Executor executor,
                       final Topology<String> topology
    ) throws IOException {
        super(getConfig(port), dao);
        this.dao = dao;
        this.executor = executor;
        this.topology = topology;
        this.nodes = topology
                .all()
                .stream()
                .collect(Collectors.toMap(node -> node,
                        node -> HttpClient.newHttpClient()));
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
     * General handler for "/v0/entity" requests.
     *
     * @param id      identifier
     * @param session created session
     * @param request incoming request
     * @see #getData(ByteBuffer)
     * @see #putData(ByteBuffer, byte[])
     * @see #deleteData(ByteBuffer)
     */
    @Path("/v0/entity")
    public void entityHandler(@Param(value = "id", required = true) final String id,
                              @Param(value = "replicas") final String replicas,
                              final HttpSession session, final Request request) {
        final int[] params = readReplicas(replicas);
        if (id.isBlank() || params[0] == 0 || params[0] > params[1]) {
            sendResponse(session, () -> BAD_REQUEST);
        } else {
            final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
            if (" True".equals(request.getHeader(PROXIED_REQUEST + ":"))) {
                sendResponse(session, () -> completeRequest(request, key));
            } else {
                sendResponse(session, () ->
                        RequestCoordinator.processResponses(sendRequest(params[1], key, request),
                                request.getMethod(), params[1] - params[0]));
            }
        }
    }

    private int[] readReplicas(final String replicas) {
        return replicas == null
                ? new int[]{topology.all().size() / 2 + 1, topology.all().size()}
                : new int[]{Character.getNumericValue(replicas.charAt(0)),
                Character.getNumericValue(replicas.charAt(2))};
    }

    private Response completeRequest(final Request request, final ByteBuffer key) throws IOException {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return getData(key);
            case Request.METHOD_PUT:
                return putData(key, request.getBody());
            case Request.METHOD_DELETE:
                return deleteData(key);
            default:
                return BAD_REQUEST;
        }
    }

    private void sendResponse(final HttpSession session, final MethodHandler method) {
        executor.execute(() -> {
            try {
                session.sendResponse(method.handle());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, e.getMessage());
                    log.error("Things goes bad", e);
                } catch (IOException ex) {
                    log.error("Things goes really bad", ex);
                }
            }
        });
    }

    private List<Response> sendRequest(final int from, final ByteBuffer key, final Request request) {
        final List<Response> responses = new ArrayList<>();
        for (int i = 0; i < from; i++) {
            final String node = topology.findNextNode(key, i);
            if (topology.isMe(node)) {
                try {
                    responses.add(completeRequest(request, key));
                } catch (IOException e) {
                    responses.add(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
                }
            } else {
                try {
                    responses.add(sendToNode(node, request));
                } catch (IOException e) {
                    responses.add(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
                }
            }
        }
        return responses;
    }

    /**
     * Method gets data by specified key.
     *
     * @param key identifier
     * @return HttpStatus.OK if found, else HttpStatus.NOT_FOUND
     * @throws IOException internal error
     */
    private Response getData(final ByteBuffer key) throws IOException {
        try {
            final ByteBuffer result = dao.get(key);
            return new Response(Response.OK, ByteBufferUtils.getByteArray(result));
        } catch (NoSuchElementExceptionLite e) {
            return new Response(Response.NOT_FOUND, e.getMessage().getBytes(Charset.defaultCharset()));
        }
    }

    /**
     * Method puts data by specified key.
     *
     * @param key  data identifier
     * @param data data
     * @return HttpStatus.CREATED
     * @throws IOException internal error
     */
    private Response putData(final ByteBuffer key, final byte[] data) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(data));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    /**
     * Method deletes data with defined key.
     *
     * @param key data identifier
     * @return HttpStatus.ACCEPTED
     * @throws IOException internal error
     */
    private Response deleteData(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
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
                         @Param(value = "end") final String end, final HttpSession session) {
        if (start.isBlank()) {
            sendResponse(session, () -> BAD_REQUEST);
        } else {
            executor.execute(() -> {
                final ByteBuffer from = ByteBuffer.wrap(start.getBytes(Charsets.UTF_8));
                final ByteBuffer to = end == null || end.isEmpty()
                        ? null : ByteBuffer.wrap(end.getBytes(Charsets.UTF_8));
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

    private Response sendToNode(final String node, final Request request) throws IOException {
        try {
            return Converter.convertHttpResponse(nodes.get(node).send(Converter.convertRequest(request, node),
                    HttpResponse.BodyHandlers.ofByteArray()));
        } catch (InterruptedException | IOException e) {
            throw new IOException("proxy troubles", e);
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

    @FunctionalInterface
    private interface MethodHandler {
        Response handle() throws IOException;
    }
}
