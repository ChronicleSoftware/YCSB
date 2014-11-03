package com.yahoo.ycsb;

import java.io.InputStream;

public class InputStreamByteIterator extends ByteIterator {
	long len;
	InputStream ins;
	long off;
	
	public InputStreamByteIterator(InputStream ins, long len) {
		this.len = len;
		this.ins = ins;
		off = 0;
	}
	
	@Override
	public boolean hasNext() {
		return off < len;
	}

	@Override
	public byte nextByte() {
		int ret;
		try {
			ret = ins.read();
		} catch(Exception e) {
			throw new IllegalStateException(e);
		}
		if(ret == -1) { throw new IllegalStateException("Past EOF!"); }
		off++;
		return (byte)ret;
	}

	@Override
	public long bytesLeft() {
		return len - off;
	}

}
