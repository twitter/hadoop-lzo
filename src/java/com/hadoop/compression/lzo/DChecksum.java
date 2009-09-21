package com.hadoop.compression.lzo;

import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Checksums on decompressed block data with header bitmask, Checksum class.
 */
public enum DChecksum {
  F_ADLER32D(0x01, Adler32.class), F_CRC32D(0x100, CRC32.class);
  private final int mask;
  private final Class<? extends Checksum> clazz;
  DChecksum(int mask, Class<? extends Checksum> clazz) {
    this.mask = mask;
    this.clazz = clazz;
  }
  public int getHeaderMask() {
    return mask;
  }
  public Class<? extends Checksum> getChecksumClass() {
    return clazz;
  }
}