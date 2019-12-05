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

    public static RocksRecord fromByteArray(final byte[] array){
        final ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return fromByteBuffer(byteBuffer);
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
                .putChar(isDead() ? 'y' : 'n').putLong(timestamp).put(data.duplicate()).array();
    }

    public ByteBuffer getData() {
        if (isDead())
            throw new NoSuchElementExceptionLite("Data is dead");
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
