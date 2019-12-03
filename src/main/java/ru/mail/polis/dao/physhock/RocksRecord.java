package ru.mail.polis.dao.physhock;

import java.nio.ByteBuffer;

/**
 * @author fshkolni
 */
public class RocksRecord {

    private final ByteBuffer data;
    private final long timestamp;
    private final boolean dead;

    public RocksRecord(final ByteBuffer data, final long timestamp, final boolean dead) {
        this.data = data;
        this.timestamp = timestamp;
        this.dead = dead;
    }

    public static RocksRecord fromByteBuffer(final ByteBuffer byteBuffer) {
        final char flag = byteBuffer.getChar();
        final long time = byteBuffer.getLong();
        return new RocksRecord(byteBuffer, time, flag == 'y');
    }

    public boolean isDead() {
        return dead;
    }

    public byte[] toByteArray() {
        return ByteBuffer.allocate(Character.BYTES + Long.BYTES + data.remaining())
                .putChar(isDead() ? 'y' : 'n').putLong(timestamp).put(data).array();
    }

    public ByteBuffer getData() {
        if (isDead())
            throw new IllegalArgumentException("Data is dead");
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
