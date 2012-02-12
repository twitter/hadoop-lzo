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

/**
 * The index is stored as follows:
 * <ul>
 * <li> 4 bytes: 1 for topmost bit, 30 0s, 1 (version number)..
 * <li> 4 bytes: first offset (offset from LZO header)
 * <li> 4 bytes: size of first block
 * <li> Sequence of 2-byte shorts: delta to size of the first split that gets you to size of next split.
 *</ul>
 */
public class LzoTinyOffsetsSerde implements LzoIndexSerde {

  private static final int BUFFER_CAPACITY = 16 * 1024 * 8; //size for a 4GB file (with 256KB lzo blocks)

  private DataOutputStream os;
  private DataInputStream is;
  private ByteBuffer bytesIn;
  private int numBlocks = 0;

  private boolean readFirstLong = false;
  private int firstBlockSize;
  private boolean wroteFirstBlock = false;

  private boolean readInitialOffset = false;
  private boolean wroteInitialOffset = false;
  private long currOffset = 0;

  private static final int VERSION = (1 << 31) + 1;

  @Override
  public boolean accepts(long firstLong) {
    if ( ((int) (firstLong >>> 32)) == VERSION) {
      currOffset = (int) (firstLong << 32 >>> 32);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void prepareToWrite(DataOutputStream os) throws IOException {
    this.os = os;
    os.writeInt(VERSION);
    wroteFirstBlock = false;
    wroteInitialOffset = false;
  }

  @Override
  public void prepareToRead(DataInputStream is) throws IOException {
    this.is = is;
    bytesIn = fillBuffer();
    int remaining = bytesIn.remaining();
    numBlocks = (remaining == 0) ? 1 :
      (remaining == 4) ? 2 :
        (bytesIn.remaining() - 4) / 2 + 2; // plus one for the first offset, plus one for block.
    readFirstLong = false;
    readInitialOffset = false;
  }

  @Override
  public void writeOffset(long offset) throws IOException {

    if (!wroteInitialOffset) {
      os.writeInt((int) offset);
      wroteInitialOffset = true;
    } else if (!wroteFirstBlock) {
      firstBlockSize = (int) (offset - currOffset);
      os.writeInt(firstBlockSize);
      wroteFirstBlock = true;
    } else {
      os.writeShort((short) ( (offset - currOffset) - firstBlockSize));
    }
    currOffset = offset;

  }

  @Override
  public void finishWriting() throws IOException {
    os.close();
  }

  @Override
  public boolean hasNext() throws IOException {
   return !readInitialOffset || !readFirstLong || (bytesIn != null && bytesIn.hasRemaining());
  }

  @Override
  public long next() throws IOException {
    if (!readInitialOffset) {
      readInitialOffset = true;
    } else if (!readFirstLong) {
      readFirstLong = true;
      firstBlockSize = bytesIn.getInt();
      currOffset += firstBlockSize;
    } else if (bytesIn != null && bytesIn.hasRemaining()) {
      currOffset += firstBlockSize + bytesIn.getShort();
    } else {
      throw new IOException("Attempt to read past the edge of the index.");
    }
    return currOffset;
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
