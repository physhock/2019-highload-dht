package ru.mail.polis.dao.physhock;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

public class DAOImpl implements DAO {

    private static final String ROCK = "RocksDB troubles";
    private final RocksDB rocksDB;

    public DAOImpl(final File path) throws IOException {
        this.rocksDB = createDB(path);
    }

    private RocksDB createDB(final File path) throws IOException {
        RocksDB.loadLibrary();
        try {
            final Options options = new Options()
                    .setCreateIfMissing(true)
                    .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
            return RocksDB.open(options, path.getAbsolutePath());
        } catch (RocksDBException e) {
            throw new IOException("Cannot create DB", e);
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {

        final RocksIterator iterator = rocksDB.newIterator();
        iterator.seek(ByteBufferUtils.restoreByteArray(from));

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                skip();
                return iterator.isValid();
            }

            private void skip() {
                while (iterator.isValid()) {
                    final byte[] value = iterator.value();
                    final RocksRecord record = RocksRecord.fromByteArray(value);
                    if (!record.isDead()) {
                        break;
                    }
                    iterator.next();
                }
            }

            @Override
            public Record next() {
                skip();
                if (!hasNext()) {
                    throw new NoSuchElementException("Next on empty iterator");
                }
                final ByteBuffer key = ByteBufferUtils.shiftByteArray(iterator.key());
                final ByteBuffer value = ByteBuffer.wrap(iterator.value());
                final RocksRecord rocksRecord = RocksRecord.fromByteBuffer(value);
                final Record record = Record.of(key, rocksRecord.getData());
                iterator.next();
                skip();
                return record;
            }
        };
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        try {
            return RocksRecord.fromByteArray(
                    Optional.ofNullable(rocksDB.get(ByteBufferUtils.restoreByteArray(key)))
                            .orElseThrow(() ->
                                    new NoSuchElementExceptionLite("This is not the data you are looking for")))
                    .getData();
        } catch (RocksDBException e) {
            throw new IOException(ROCK, e);
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        try {
            rocksDB.put(ByteBufferUtils.restoreByteArray(key),
                    new RocksRecord(value, System.currentTimeMillis(), false).toByteArray());
        } catch (RocksDBException e) {
            throw new IOException(ROCK, e);
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        try {
            rocksDB.put(ByteBufferUtils.restoreByteArray(key),
                    new RocksRecord(ByteBuffer.allocate(0), System.currentTimeMillis(), true).toByteArray());
        } catch (RocksDBException e) {
            throw new IOException(ROCK, e);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            rocksDB.compactRange();
        } catch (RocksDBException exception) {
            throw new IOException(ROCK, exception);
        }
    }

    @Override
    public void close() {
        rocksDB.close();
    }
}
