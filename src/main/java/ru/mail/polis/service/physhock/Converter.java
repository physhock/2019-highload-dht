package ru.mail.polis.service.physhock;

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
 * @author fshkolni
 */
public class Converter {

    public static Response convertHttpResponse(final HttpResponse<byte[]> httpResponse) {
        return new Response(String.valueOf(httpResponse.statusCode()), httpResponse.body());
    }

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

    private static String convertRequestMethod(Request request) {
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
