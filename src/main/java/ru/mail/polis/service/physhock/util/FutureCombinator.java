package ru.mail.polis.service.physhock.util;

import one.nio.http.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Utility class for making ack-element subset all nodes set.
 *
 * @author fshkolni
 */
public class FutureCombinator {

    private FutureCombinator() {
    }

    /**
     * Method takes set of all responses and make all possible combinations of them with defined size.
     *
     * @param futures set of all responses
     * @param ack     size of subset
     * @return list of all possible ack-subsets
     */
    public static List<List<CompletableFuture<Response>>> combineFutures(
            final List<CompletableFuture<Response>> futures, final int ack) {
        final List<List<CompletableFuture<Response>>> combinations = new ArrayList<>();
        final List<CompletableFuture<Response>> data = new ArrayList<>(futures.subList(0, ack));
        helper(combinations, futures, data, 0, 0);
        return combinations;
    }

    private static void helper(final List<List<CompletableFuture<Response>>> combinations,
                               final List<CompletableFuture<Response>> futures,
                               final List<CompletableFuture<Response>> data,
                               final int start, final int index) {
        if (index == data.size()) {
            combinations.add(new ArrayList<>(data.subList(0, data.size())));
        } else if (start <= futures.size() - 1) {
            data.set(index, futures.get(start));
            helper(combinations, futures, data, start + 1, index + 1);
            helper(combinations, futures, data, start + 1, index);
        }
    }
}
