package org.nessdb;

public class DBException extends Exception {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

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