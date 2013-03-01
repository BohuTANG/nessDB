package org.nessdb;

public class DB {

	static {
		loadNativeLib();
	}

	private static void loadNativeLib() {
		System.load("/home/yanghu/Project/nessdb/bindings/java/libnessdb_jni.so");
		//System.loadLibrary("nessdb_jni");
	}

	private String path;
	private long ptr;

	public DB(String path) {
		this.path = path;
	}

	public String getPath() {
		return path;
	}

	public void open() throws DBException {
		ptr = open(this.path);
		if (ptr == 0) {
			throw new DBException("open db failed.");
		}
	}

	public void set(byte[] key, int klen, byte[] value, int vlen)
			throws DBException {
		if (ptr == 0) {
			throw new DBException("open db first.");
		}
		set(ptr, key, klen, value, vlen);
	}

	public byte[] get(byte[] key, int klen) throws DBException {
		if (ptr == 0) {
			throw new DBException("open db first.");
		}
		return get(ptr, key, klen);
	}

	public void remove(byte[] key, int klen) throws DBException {
		if (ptr == 0) {
			throw new DBException("open db first.");
		}
		remove(ptr, key, klen);
	}

	public String info() {
		return null;
	}

	public void close() {
		if (ptr != 0) {
			close(this.ptr);
		}
	}

	private native long open(String sst_path);

	private native int set(long ptr, byte[] key, int klen, byte[] value, int vlen);

	private native byte[] get(long ptr, byte[] key, int klen);

	private native int remove(long ptr, byte[] key, int klen);

	private native void info(long ptr, byte[] infos, int vlen);

	private native void close(long ptr);
}
