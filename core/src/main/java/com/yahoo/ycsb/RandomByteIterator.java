package com.yahoo.ycsb;

/**
 *  A ByteIterator that generates a random sequence of bytes.
 */
public class RandomByteIterator extends ByteIterator {
  private long len;
  private long off;
  private int bufOff;
  private byte[] buf;

  @Override
  public boolean hasNext() {
    return (off + bufOff) < len;
  }

  private void fillBytesImpl(byte[] buffer, int base) {
    int bytes = Utils.random().nextInt();
    try {
      buffer[base+0] = (byte)(((bytes) & 31) + ' ');
      buffer[base+1] = (byte)(((bytes >> 5) & 31) + ' ');
      buffer[base+2] = (byte)(((bytes >> 10) & 31) + ' ');
      buffer[base+3] = (byte)(((bytes >> 15) & 31) + ' ');
      buffer[base+4] = (byte)(((bytes >> 20) & 31) + ' ');
      buffer[base+5] = (byte)(((bytes >> 25) & 31) + ' ');
    } catch (ArrayIndexOutOfBoundsException e) { /* ignore it */ }
  }

  private void fillBytes() {
    if(bufOff ==  buf.length) {
      fillBytesImpl(buf, 0);
      bufOff = 0;
      off += buf.length;
    }
  }

  public RandomByteIterator(long len) {
    this.len = len;
    this.buf = new byte[6];
    this.bufOff = buf.length;
    fillBytes();
    this.off = 0;
  }

  public byte nextByte() {
    fillBytes();
    bufOff++;
    return buf[bufOff-1];
  }

  @Override
  public int nextBuf(byte[] buffer, int bufferOffset) {
    int ret;
    if(len - off < buffer.length - bufferOffset) {
      ret = (int)(len - off);
    } else {
      ret = buffer.length - bufferOffset;
    }
    int i;
    for(i = 0; i < ret; i+=6) {
      fillBytesImpl(buffer, i + bufferOffset);
    }
    off+=ret;
    return ret + bufferOffset;
  }

  @Override
  public long bytesLeft() {
    return len - off - bufOff;
  }
}
