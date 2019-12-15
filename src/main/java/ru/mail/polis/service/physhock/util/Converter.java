package ru.mail.polis.service.physhock.util;

import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.util.Utf8;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

/**
 * Utility class for converting one.nio.http to java.net.http.
 *
 * @author fshkolni
 */
public class Converter {

    private Converter() {
    }

    /**
     * Method converts java.net.http.HttpResponse to one.nio.http.Response.
     *
     * @param httpResponse response to convert
     * @return converted response
     */
    public static Response convertHttpResponse(final HttpResponse<byte[]> httpResponse) {
        return new Response(String.valueOf(httpResponse.statusCode()), httpResponse.body());
    }

    /**
     * Method converts one.nio.request to java.net.http.HttpRequest.
     *
     * @param request request to convert
     * @param node    request destination
     * @return converted request
     */
    public static HttpRequest convertRequest(final Request request, final String node) {
        return HttpRequest.newBuilder()
                .timeout(Duration.ofSeconds(2))
                .header("Request-from-node", "True")
                .method(convertRequestMethod(request),
                        request.getBody() == null
                                ? HttpRequest.BodyPublishers.noBody()
                                : HttpRequest.BodyPublishers.ofByteArray(request.getBody()))
                .uri(URI.create(node + request.getURI())).build();
    }

    private static String convertRequestMethod(final Request request) {
        final byte[] bytes = request.toBytes();
        final int bodyLength = request.getBody() == null ? 0 : request.getBody().length;
        final int length = Utf8.length(request.getURI()) + 13 + request.getHeaderCount() * 2 + bodyLength + 1;
        return Utf8.read(bytes, 0,
                bytes.length - (Arrays.stream(request.getHeaders())
                        .filter(Objects::nonNull)
                        .mapToInt(String::length)
                        .sum() + length));
    }
}
