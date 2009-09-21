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
    return this.chkCMap.size() + this.chkDMap.size();
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
