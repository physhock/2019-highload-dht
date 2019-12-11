package ru.mail.polis.service.physhock;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.dao.physhock.RocksRecord;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author fshkolni
 */
class RequestCoordinator {

    public static Response processRequest(final List<List<CompletableFuture<Response>>> combinedRequests,
                                          final int requestMethod) {
        return getResponse(CompletableFuture.anyOf(combinedRequests.stream()
                .map(futures -> CompletableFuture.allOf(futures.toArray(
                        new CompletableFuture<?>[0])))
                .toArray(CompletableFuture[]::new))
                .thenApply(future -> combinedRequests.stream()
                        .filter(list -> CompletableFuture.allOf(list.toArray(
                                new CompletableFuture<?>[0])).isDone())
                        .findFirst()
                        .orElseThrow()
                        .stream()
                        .map(RequestCoordinator::getResponse)
                        .collect(Collectors.toList()))
                .thenApply(responseList -> processResponses(requestMethod, responseList)));
    }

    private static Response processResponses(final int requestMethod, final List<Response> responseList) {
        if (responseList.stream()
                .anyMatch(response -> response.getStatus() == 500)) {
            return new Response("504 Not Enough Replicas", Response.EMPTY);
        } else {
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

    private static Response getResponse(final CompletableFuture<Response> responseCompletableFuture) {
        try {
            return responseCompletableFuture.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }
}
