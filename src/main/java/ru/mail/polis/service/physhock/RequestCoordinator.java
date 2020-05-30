package ru.mail.polis.service.physhock;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.dao.physhock.RocksRecord;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;

/**
 * Class for processing replicas requests.
 *
 * @author fshkolni
 */
class RequestCoordinator {

    private RequestCoordinator() {
    }

    public static Response processResponses(final List<Response> responseList,
                                            final int requestMethod, final int losses) {
        if (responseList.stream().filter(response -> response.getStatus() == 500).count() > losses) {
            return new Response("504 Not enough replicas", Response.EMPTY);
        }
        switch (requestMethod) {
            case Request.METHOD_GET:
                if (responseList.stream().allMatch(response -> response.getStatus() == 404)
                        || responseList.stream().anyMatch(response -> response.getBody().length != 0
                        && new String(response.getBody(), StandardCharsets.UTF_8).equals("Data is dead"))) {
                    return new Response(Response.NOT_FOUND, Response.EMPTY);
                } else {
                    return responseList.stream()
                            .filter(response -> response.getStatus() == 200)
                            .max(Comparator.comparingLong(response ->
                                    RocksRecord.fromByteArray(response.getBody()).getTimestamp()))
                            .orElseThrow();
                }
            case Request.METHOD_PUT:
                return new Response(Response.CREATED, Response.EMPTY);
            case Request.METHOD_DELETE:
                return new Response(Response.ACCEPTED, Response.EMPTY);
            default:
                return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
    }
}
