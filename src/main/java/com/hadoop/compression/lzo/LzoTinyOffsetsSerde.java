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
import java.io.EOFException;
import java.io.IOException;

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

  private DataOutputStream os;
  private DataInputStream is;

  private boolean readFirstLong = false;
  private int firstBlockSize;
  private boolean wroteFirstBlock = false;

  private boolean readInitialOffset = false;
  private boolean wroteInitialOffset = false;
  private long currOffset = 0;

  // for hasNext, we have to read the next value
  // (or, if the buffer hasn't been used, just check it for null)
  private Integer bufferedOffset = null;

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
      int delta = ((int) (offset - currOffset)) - firstBlockSize;
      Varint.writeSignedVarInt(delta, os);
    }
    currOffset = offset;

  }

  @Override
  public void finishWriting() throws IOException {
    os.close();
  }

  @Override
  public void finishReading() throws IOException {
    is.close();
  }

  @Override
  public boolean hasNext() throws IOException {
    if (readInitialOffset && readFirstLong) {
      if (bufferedOffset == null) {
        // try to read something. If we hit EOF, we are done.
        try {
          bufferedOffset = Varint.readSignedVarInt(is);
        } catch (EOFException e) {
          return false;
        }
      }
      return true;
    }
    return !readInitialOffset || (!readFirstLong && is.available() != 0);
  }

  @Override
  public long next() throws IOException {
    if (!readInitialOffset) {
      readInitialOffset = true;
    } else if (!readFirstLong) {
      readFirstLong = true;
      firstBlockSize = is.readInt();
      currOffset += firstBlockSize;
    } else {
      if (bufferedOffset == null) {
        bufferedOffset = Varint.readSignedVarInt(is);
      }
      currOffset += firstBlockSize + bufferedOffset;
      bufferedOffset = null;
    }
    return currOffset;
  }

}
