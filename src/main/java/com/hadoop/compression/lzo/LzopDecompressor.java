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

import java.io.IOException;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.zip.Checksum;

public class LzopDecompressor extends LzoDecompressor {

  private final EnumMap<DChecksum,Checksum> chkDMap = new EnumMap<DChecksum,Checksum>(DChecksum.class);
  private final EnumMap<CChecksum,Checksum> chkCMap = new EnumMap<CChecksum,Checksum>(CChecksum.class);

  /**
   * Create an LzoDecompressor with LZO1X strategy (the only lzo algorithm
   * supported by lzop).
   */
  public LzopDecompressor(int bufferSize) {
    super(LzoDecompressor.CompressionStrategy.LZO1X_SAFE, bufferSize);
  }

  /**
   * Given a set of decompressed and compressed checksums,
   */
  public void initHeaderFlags(EnumSet<DChecksum> dflags,
      EnumSet<CChecksum> cflags) {
    try {
      for (DChecksum flag : dflags) {
        chkDMap.put(flag, flag.getChecksumClass().newInstance());
      }
      for (CChecksum flag : cflags) {
        chkCMap.put(flag, flag.getChecksumClass().newInstance());
      }
    } catch (InstantiationException e) {
      throw new RuntimeException("Internal error", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Internal error", e);
    }
  }

  /**
   * Get the number of checksum implementations
   * the current lzo file uses.
   * @return Number of checksum implementations in use.
   */
  public int getChecksumsCount() {
    return getCompressedChecksumsCount() + getDecompressedChecksumsCount();
  }

  /**
   * Get the number of compressed checksum implementations
   * the current lzo file uses.
   * @return Number of compressed checksum implementations in use.
   */
  public int getCompressedChecksumsCount() {
    return this.chkCMap.size();
  }

  /**
   * Get the number of decompressed checksum implementations
   * the current lzo file uses.
   * @return Number of decompressed checksum implementations in use.
   */
  public int getDecompressedChecksumsCount() {
    return this.chkDMap.size();
  }

  /**
   * Reset all checksums registered for this decompressor instance.
   */
  public synchronized void resetChecksum() {
    for (Checksum chk : chkDMap.values()) chk.reset();
    for (Checksum chk : chkCMap.values()) chk.reset();
  }

  /**
   * Given a checksum type, verify its value against that observed in
   * decompressed data.
   */
  public synchronized boolean verifyDChecksum(DChecksum typ, int checksum) {
    return (checksum == (int)chkDMap.get(typ).getValue());
  }

  /**
   * Given a checksum type, verity its value against that observed in
   * compressed data.
   */
  public synchronized boolean verifyCChecksum(CChecksum typ, int checksum) {
    return (checksum == (int)chkCMap.get(typ).getValue());
  }

  @Override
  public synchronized void setInput(byte[] b, int off, int len) {
    if (!isCurrentBlockUncompressed()) {
      // If the current block is uncompressed, there was no compressed
      // checksum and no compressed data, so nothing to update.
      for (Checksum chk : chkCMap.values()) chk.update(b, off, len);
    }
    super.setInput(b, off, len);
  }

  @Override
  public synchronized int decompress(byte[] b, int off, int len)
  throws IOException {
    int ret = super.decompress(b, off, len);
    if (ret > 0) {
      for (Checksum chk : chkDMap.values()) chk.update(b, off, ret);
    }
    return ret;
  }
}
