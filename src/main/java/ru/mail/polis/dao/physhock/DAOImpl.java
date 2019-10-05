package ru.mail.polis.dao.physhock;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class DAOImpl implements DAO {

    private final RocksDB rocksDB;

    public DAOImpl(File path) throws IOException {
        rocksDB = createDB(path);
    }

    private RocksDB createDB(File path) throws IOException {
        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        try (final Options options = new Options().setCreateIfMissing(true)) {
            // a factory method that returns a RocksDB instance
            try (final RocksDB db = RocksDB.open(options, path.getPath())) {
                return db;
            }
        } catch (RocksDBException e) {
            throw new IOException("Cannot create DB");
        }
    }


    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return null;
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        return null;
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {

    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
