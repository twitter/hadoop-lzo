package com.hadoop.compression.lzo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;

public class LzoBasicIndexSerde implements LzoIndexSerde {

  private static final int BUFFER_CAPACITY = 16 * 1024 * 8; //size for a 4GB file (with 256KB lzo blocks)

  private DataOutputStream os;
  private DataInputStream is;
  private ByteBuffer bytesIn;
  private long firstLong;
  private int numBlocks = 0;
  private boolean processedFirstLong = false;

  @Override
  public boolean accepts(long firstLong) {
    if (firstLong < 0) {
      return false;
    } else {
      this.firstLong = firstLong;
      return true;
    }
  }

  @Override
  public void prepareToWrite(DataOutputStream os) throws IOException {
    this.os = os;
  }

  @Override
  public void prepareToRead(DataInputStream is) throws IOException {
    this.is = is;
    bytesIn = fillBuffer();
    numBlocks = bytesIn.remaining()/8 + 1; // plus one for the first long.
    processedFirstLong = false;
  }

  @Override
  public void writeOffset(long offset) throws IOException {
    os.writeLong(offset);
  }

  @Override
  public void finishWriting() throws IOException {
    os.close();
  }

  @Override
  public boolean hasNext() throws IOException {
   return !processedFirstLong || (bytesIn != null && bytesIn.hasRemaining());
  }

  @Override
  public long next() throws IOException {
    if (!processedFirstLong) {
      processedFirstLong = true;
      return firstLong;
    }
    if (bytesIn != null && bytesIn.hasRemaining()) {
      return bytesIn.getLong();
    } else {
      throw new IOException("Attempt to read past the edge of the index.");
    }
  }

  private ByteBuffer fillBuffer() throws IOException {
    DataOutputBuffer bytes = new DataOutputBuffer(BUFFER_CAPACITY);
    // copy indexIn and close it if finished
    IOUtils.copyBytes(is, bytes, 4*1024, true);
    return ByteBuffer.wrap(bytes.getData(), 0, bytes.getLength());
  }

  @Override
  public int numBlocks() {
    return numBlocks;
  }

}
