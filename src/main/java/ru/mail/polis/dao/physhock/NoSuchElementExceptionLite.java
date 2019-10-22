package ru.mail.polis.dao.physhock;

import java.util.NoSuchElementException;


public class NoSuchElementExceptionLite extends NoSuchElementException {
    private static final long serialVersionUID = 1L;

    public NoSuchElementExceptionLite(final String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
