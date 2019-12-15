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
     * @param ack size of subset
     * @return list of all possible ack-subsets
     */
    public static List<List<CompletableFuture<Response>>> combineFutures(
            final List<CompletableFuture<Response>> futures, final int ack) {
        final List<List<CompletableFuture<Response>>> combinations = new ArrayList<>();
        final List<CompletableFuture<Response>> data = new ArrayList<>(ack);
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
