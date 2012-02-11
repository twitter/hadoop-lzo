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

public interface LzoIndexSerde {

  /**
   * Serdes will be tried in order until one is found that accepts
   * the offered format. A format is determined from the first 8
   * bytes (represented as a long) written to the index file.
   * <p>
   * The first long is somewhat constrained: the topmost bit should be
   * 1, the next 31 are a version number by which the appropriate SerDe
   * is decided, and the next 32 can have arbitrary data (a header, or
   * a length of the header, or an offset.. up to you).
   *
   * @param firstLong
   * @return true if this format is recognized by the SerDe, false otherwise.
   */
  public boolean accepts(long firstLong);

  public void prepareToWrite(DataOutputStream os) throws IOException;

  /**
   * Prepare to read the index. Note that the first 8 bits will have been already
   * read from this stream, and passed to you in accepts() in the form of a long.
   * @param is InputStream to read.
   */
  public void prepareToRead(DataInputStream is) throws IOException;

  /**
   * Write the next offset into the file. It is expected that
   * the offsets are supplied in order. <code>prepareToWrite()</code>
   * should be called before the first invocation of this method.
   * @param offset
   */
  public void writeOffset(long offset) throws IOException;

  public void finishWriting() throws IOException;

  public boolean hasNext() throws IOException;

  public long next() throws IOException;

  /**
   * Get the number of block expected to be read from this index.
   * Will only be called after prepareToRead().
   * @return number of block offsets that will be read back.
   */
  public int numBlocks();

}
