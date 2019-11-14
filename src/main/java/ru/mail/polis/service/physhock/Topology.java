package ru.mail.polis.service.physhock;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Measuring node from the given key.
 */
public interface Topology<T> {

    boolean isMe(final T node);

    T calculateFor(final ByteBuffer key);

    T findNextNode(final ByteBuffer key, final int offset);

    Set<T> all();
}
