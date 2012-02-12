/*
 * This file is part of Hadoop-Gpl-Compression.
 *
 * Hadoop-Gpl-Compression is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Hadoop-Gpl-Compression is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Hadoop-Gpl-Compression.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

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
