package com.hadoop.compression.lzo;

import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Checksums on compressed block data with header bitmask, Checksum class.
 */
public enum CChecksum {
  F_ADLER32C(0x02, Adler32.class), F_CRC32C(0x200, CRC32.class);
  private final int mask;
  private final Class<? extends Checksum> clazz;
  CChecksum(int mask, Class<? extends Checksum> clazz) {
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