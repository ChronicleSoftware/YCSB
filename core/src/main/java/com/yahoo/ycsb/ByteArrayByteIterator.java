package com.yahoo.ycsb;

public class ByteArrayByteIterator extends ByteIterator {
	byte[] str;
	int off;
	final int len;
	public ByteArrayByteIterator(byte[] s) {
		this.str = s;
		this.off = 0;
		this.len = s.length;
	}

	public ByteArrayByteIterator(byte[] s, int off, int len) {
		this.str = s;
		this.off = off;
		this.len = off + len;
	}

	@Override
	public boolean hasNext() {
		return off < len;
	}

	@Override
	public byte nextByte() {
		byte ret = str[off];
		off++;
		return ret;
	}

	@Override
	public long bytesLeft() {
		return len - off;
	}

}
