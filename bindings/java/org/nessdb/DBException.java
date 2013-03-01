package org.nessdb;

public class DBException extends Exception {
    public DBException() {
    }

    public DBException(String s) {
        super(s);
    }

    public DBException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public DBException(Throwable throwable) {
        super(throwable);
    }
}