package ru.mail.polis.dao.physhock;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class ByteBufferUtils {
    private ByteBufferUtils() {
    }

    public static ByteBuffer shiftByteArray(@NotNull final byte[] byteArray) {
        final byte[] body = byteArray.clone();

        for (int i = 0; i < body.length; i++) {
            body[i] += Byte.MIN_VALUE;
        }

        return ByteBuffer.wrap(body);
    }

    public static byte[] restoreByteArray(@NotNull final ByteBuffer buffer) {
        final byte[] body = getByteArray(buffer);

        for (int i = 0; i < body.length; i++) {
            body[i] -= Byte.MIN_VALUE;
        }
        return body;
    }

    public static byte[] getByteArray(@NotNull final ByteBuffer buffer) {
        final ByteBuffer duplicate = buffer.duplicate();
        final byte[] body = new byte[duplicate.remaining()];

        duplicate.get(body);

        return body;
    }

}
