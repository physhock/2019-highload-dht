package ru.mail.polis.dao.physhock;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class ByteBufferUtils {
    private ByteBufferUtils() {
    }

    /**
     * Method shifts byte array then wrap it to the byte buffer.
     *
     * @param byteArray array to shift
     * @return shifted array wrapped into buffer
     */
    public static ByteBuffer shiftByteArray(@NotNull final byte[] byteArray) {
        final byte[] body = byteArray.clone();

        for (int i = 0; i < body.length; i++) {
            body[i] += Byte.MIN_VALUE;
        }

        return ByteBuffer.wrap(body);
    }

    /**
     * Method restore shifted byte array from ByteBuffer.
     *
     * @param buffer modified array wrapped into buffer
     * @return restored byte array
     */
    public static byte[] restoreByteArray(@NotNull final ByteBuffer buffer) {
        final byte[] body = getByteArray(buffer);

        for (int i = 0; i < body.length; i++) {
            body[i] -= Byte.MIN_VALUE;
        }
        return body;
    }

    /**
     * Method extract byte array from byte buffer in safe manner.
     *
     * @param buffer buffer
     * @return extracted byte array
     */
    public static byte[] getByteArray(@NotNull final ByteBuffer buffer) {
        final ByteBuffer duplicate = buffer.duplicate();
        final byte[] body = new byte[duplicate.remaining()];

        duplicate.get(body);

        return body;
    }
}
