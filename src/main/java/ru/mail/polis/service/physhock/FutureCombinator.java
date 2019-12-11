package ru.mail.polis.service.physhock;

import one.nio.http.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author fshkolni
 */
public class FutureCombinator {

    public static List<List<CompletableFuture<Response>>> combineFutures(
            final List<CompletableFuture<Response>> futures, final int ack) {
        List<List<CompletableFuture<Response>>> combinations = new ArrayList<>();
        List<CompletableFuture<Response>> data = new ArrayList<>(ack);
        helper(combinations, futures, data, 0, futures.size() - 1, 0, ack);
        return combinations;
    }

    private static void helper(final List<List<CompletableFuture<Response>>> combinations,
                               final List<CompletableFuture<Response>> futures,
                               final List<CompletableFuture<Response>> data,
                               final int start, final int end, final int index, final int ack) {
        if (index == ack) {
            combinations.add(new ArrayList<>(data.subList(0, ack)));
        } else if (start <= end) {
            data.add(index, futures.get(start));
            helper(combinations, futures, data, start + 1, end, index + 1, ack);
            helper(combinations, futures, data, start + 1, end, index, ack);
        }
    }
}
