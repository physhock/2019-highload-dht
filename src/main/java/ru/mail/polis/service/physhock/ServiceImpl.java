package ru.mail.polis.service.physhock;

import com.google.common.base.Charsets;
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
import one.nio.util.Utf8;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.physhock.ByteBufferUtils;
import ru.mail.polis.dao.physhock.NoSuchElementExceptionLite;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Implementation of Service.
 */
public class ServiceImpl extends HttpServer implements Service {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final Response BAD_REQUEST = new Response(Response.BAD_REQUEST, Response.EMPTY);
    private static final String SKYNET_CHECK = "Request-from-node";
    private final DAO dao;
    private final Executor executor;
    private final Topology<String> topology;
    private final HttpClient client;

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
                       final HttpClient client,
                       final Topology<String> topology
    ) throws IOException {
        super(getConfig(port), dao);
        this.dao = dao;
        this.executor = executor;
        this.client = client;
        this.topology = topology;
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
                              final HttpSession session,
                              final Request request) {
        if (id.isBlank()) {
            sendResponse(session, () -> BAD_REQUEST);
        } else {
            final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
            final String node = topology.calculateFor(key);
            if (" True".equals(request.getHeader(SKYNET_CHECK + ":"))) {
                switch (request.getMethod()) {
                    case Request.METHOD_GET:
                        sendResponse(session, () -> getData(key));
                        break;
                    case Request.METHOD_PUT:
                        sendResponse(session, () -> putData(key, request.getBody()));
                        break;
                    case Request.METHOD_DELETE:
                        sendResponse(session, () -> deleteData(key));
                        break;
                    default:
                        sendResponse(session, () -> BAD_REQUEST);
                        break;
                }
            } else if (topology.isMe(node) && request.getHeader(SKYNET_CHECK) == null) {
                coordinateRequest(
                        key,
                        Optional.ofNullable(replicas).orElse("2/3"),
                        session,
                        request);
            } else {
                sendToNode(node, request)
                        .thenAccept(response -> sendResponse(session, () -> response));
            }
        }
    }

    private Response convertHttpResponse(final HttpResponse<byte[]> httpResponse) {
        return new Response(String.valueOf(httpResponse.statusCode()), httpResponse.body());
    }

    private HttpRequest convertRequest(final Request request, final String node) {
        return HttpRequest.newBuilder()
                .timeout(Duration.ofSeconds(10))
                .header(SKYNET_CHECK, "True")
                .method(convertRequestMethod(request),
                        request.getBody() == null
                                ? HttpRequest.BodyPublishers.noBody()
                                : HttpRequest.BodyPublishers.ofByteArray(request.getBody()))
                .uri(URI.create(node + request.getURI())).build();
    }

    private String convertRequestMethod(Request request) {
        final byte[] bytes = request.toBytes();
        final int bodyLength = request.getBody() == null ? 0 : request.getBody().length;
        final int length = Utf8.length(request.getURI())
                + 13
                + request.getHeaderCount() * 2
                + bodyLength
                + 1;
        return Utf8.read(bytes, 0,
                bytes.length - (Arrays.stream(request.getHeaders())
                        .filter(Objects::nonNull)
                        .mapToInt(String::length)
                        .sum() + length));
    }

    private void coordinateRequest(final ByteBuffer key,
                                   final String replicas,
                                   final HttpSession session,
                                   final Request request) {


        final int ack = Character.getNumericValue(replicas.charAt(0));
        final int from = Character.getNumericValue(replicas.charAt(2));
        final List<CompletableFuture<Response>> responses = new ArrayList<>();

        for (int i = 0; i < from; i++) {
            final String node = topology.findNextNode(key, i + 1);
            responses.add(sendToNode(node, request));
        }


        
        final CompletableFuture[] completableFutures = {responses.get(0), responses.get(1)};

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                CompletableFuture.anyOf(completableFutures)
                        .thenApply(future -> responses.stream()
                                .filter(CompletableFuture::isDone)
                                .map(responseCompletableFuture -> {
                                    try {
                                        return responseCompletableFuture.get(10, TimeUnit.SECONDS);
                                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                        e.printStackTrace();
                                        return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
                                    }
                                })
                                .collect(Collectors.toList()))
                        .thenAccept(list -> {
                            if (list.stream().filter(response -> response.getStatus() == 200).count() >= ack) {
                                sendResponse(session, () -> new Response(Response.OK, list.get(0).getBody()));
                            } else if (list.stream().filter(response -> response.getStatus() == 404).count() == ack) {
                                sendResponse(session, () -> new Response(Response.NOT_FOUND, Response.EMPTY));
                            } else {
                                sendResponse(session, () -> new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                            }
                        });
                break;
            case Request.METHOD_PUT:
                CompletableFuture.allOf(completableFutures)
                        .thenApply(future -> responses.stream()
                                .filter(CompletableFuture::isDone)
                                .map(responseCompletableFuture -> {
                                    try {
                                        return responseCompletableFuture.get(10, TimeUnit.SECONDS);
                                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                        e.printStackTrace();
                                        return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
                                    }
                                })
                                .collect(Collectors.toList()))
                        .thenAccept(list -> {
                            if (list.stream().filter(response -> response.getStatus() == 201).count() >= ack) {
                                sendResponse(session, () -> new Response(Response.CREATED, Response.EMPTY));
                            } else {
                                sendResponse(session, () -> new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                            }
                        });
                break;
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
            return new Response(Response.NOT_FOUND, Response.EMPTY);
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
                         @Param(value = "end") final String end,
                         final HttpSession session) {
        if (start.isBlank()) {
            sendResponse(session, () -> BAD_REQUEST);
        } else {
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

    private CompletableFuture<Response> sendToNode(final String node, final Request request) {
        final HttpRequest request1 = convertRequest(request, node);
        return client.sendAsync(request1,
                HttpResponse.BodyHandlers.ofByteArray()).thenApply(this::convertHttpResponse);
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
