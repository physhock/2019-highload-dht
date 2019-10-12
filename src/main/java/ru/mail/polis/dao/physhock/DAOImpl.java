package ru.mail.polis.dao.physhock;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

public class DAOImpl implements DAO {

    private final RocksDB rocksDB;
    private static final String ROCK = "RocksDB troubles";

    public DAOImpl(final File path) throws IOException {
        this.rocksDB = createDB(path);
    }

    private RocksDB createDB(final File path) throws IOException {
        RocksDB.loadLibrary();
        try {
            final Options options = new Options()
                    .prepareForBulkLoad()
                    .setCreateIfMissing(true)
                    .setComparator(new BytewiseComparator(new ComparatorOptions()));
            return RocksDB.open(options, path.getAbsolutePath());
        } catch (RocksDBException e) {
            throw new IOException("Cannot create DB");
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {

        final RocksIterator iterator = rocksDB.newIterator();
        iterator.seek(from.array());

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.isValid();
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("Next on empty iterator");
                }
                final ByteBuffer key = ByteBuffer.wrap(iterator.key());
                final ByteBuffer value = ByteBuffer.wrap(iterator.value());
                final Record record = Record.of(key, value);
                iterator.next();
                return record;
            }
        };
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        try {
            return ByteBuffer.wrap(
                    Optional.ofNullable(rocksDB.get(key.array()))
                            .orElseThrow(() ->
                                    new NoSuchElementExceptionLite("This is not the data you are looking for")));
        } catch (RocksDBException e) {
            throw new IOException(ROCK, e);
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        try {
            rocksDB.put(key.array(), value.array());
        } catch (RocksDBException e) {
            throw new IOException(ROCK, e);
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        try {
            rocksDB.delete(key.array());
        } catch (RocksDBException e) {
            throw new IOException(ROCK, e);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            rocksDB.compactRange();
        } catch (RocksDBException exception) {
            throw new IOException("Error while compact", exception);
        }
    }

    @Override
    public void close() {
        rocksDB.close();
    }
}
