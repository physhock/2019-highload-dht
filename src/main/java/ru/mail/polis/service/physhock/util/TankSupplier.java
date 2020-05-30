package ru.mail.polis.service.physhock.util;

import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class TankSupplier {
    private static final int VALUE_LENGTH = 256;
    private static final String LINE = "\r\n";


    @NotNull
    private static byte[] randomValue() {
        final byte[] result = new byte[VALUE_LENGTH];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    private static void put(final long requests) throws IOException {
        final String key = String.valueOf(requests);
        final byte[] value = randomValue();
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, StandardCharsets.US_ASCII)) {
            writer.write("PUT /v0/entity?id=" + key + " HTTP/1.1" + LINE);
            writer.write("Content-Length: " + value.length + LINE);
            writer.write(LINE);
        }
        request.write(value);
        System.out.write(Integer.toString(request.size()).getBytes(StandardCharsets.US_ASCII));
        System.out.write(" put\n".getBytes(StandardCharsets.US_ASCII));
        request.writeTo(System.out);
        System.out.write(LINE.getBytes(StandardCharsets.US_ASCII));
    }

    private static void get(final long requests) throws IOException {
        final String key = String.valueOf(requests);
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, StandardCharsets.US_ASCII)) {
            writer.write("GET /v0/entity?id=" + key + " HTTP/1.1\r\n");
            writer.write(LINE);
        }
        System.out.write(Integer.toString(request.size()).getBytes(StandardCharsets.US_ASCII));
        System.out.write(" get\n".getBytes(StandardCharsets.US_ASCII));
        request.writeTo(System.out);
        System.out.write(LINE.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Main function for start AmmoGenerator.
     *
     * @param args - parameters
     * @throws IOException - may be thrown IOException
     */
    public static void main(final String[] args) throws IOException {
        final String mode = args[0];
        final long requests = Long.parseLong(args[1]);

        switch (mode) {
            case "put1":
                put1(requests);
                break;
            case "put2":
                put2(requests);
                break;
            case "get1":
                get1(requests);
                break;
            case "get2":
                get2(requests);
                break;
            case "getput":
                getPut(requests);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported mode: " + mode);
        }
    }

    private static void put1(final long requests) throws IOException {
        for (int i = 0; i < requests; i++) {
            put(i);
        }
    }

    private static void put2(final long requests) throws IOException {
        int key = 0;
        for (long i = 0; i < requests; i++) {
            if (requests % 10 == 5) {
                put(nextLong(key));
            } else {
                put(key++);
            }
        }
    }

    private static void get1(final long requests) throws IOException {
        for (int i = 0; i < requests; i++) {
            get(nextLong(requests));
        }
    }

    private static void get2(final long requests) throws IOException {
        for (int i = 0; i < requests; i++) {
            final int random = ThreadLocalRandom.current().nextInt(10);
            if (random > 2) {
                get(ThreadLocalRandom.current().nextLong(requests / 10 * 9, requests));
            } else {
                get(ThreadLocalRandom.current().nextLong(requests / 10 * 9));
            }
        }
    }

    private static void getPut(final long requests) throws IOException {
        long key = 0;
        put(++key);
        for (int i = 1; i < requests; i++) {
            if (ThreadLocalRandom.current().nextBoolean()) {
                put(++key);
            } else {
                get(nextLong(key));
            }
        }
    }

    private static long nextLong(final long bound) {
        return ThreadLocalRandom.current().nextLong(bound);
    }
}
