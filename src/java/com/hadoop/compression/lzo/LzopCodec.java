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
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * A {@link CompressionCodec} for a streaming
 * <b>lzo</b> compression/decompression pair compatible with lzop.
 * http://www.lzop.org/
 */
public class LzopCodec extends LzoCodec {

  /** 9 bytes at the top of every lzo file */
  public static final byte[] LZO_MAGIC = new byte[] {
    -119, 'L', 'Z', 'O', 0, '\r', '\n', '\032', '\n' };
  /** Version of lzop this emulates */
  public static final int LZOP_VERSION = 0x1010;
  /** Latest verion of lzop this should be compatible with */
  public static final int LZOP_COMPAT_VERSION = 0x0940;

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out,
          Compressor compressor) throws IOException {
    if (!isNativeLzoLoaded(getConf())) {
      throw new RuntimeException("native-lzo library not available");
    }
    LzoCompressor.CompressionStrategy strategy =
      LzoCompressor.CompressionStrategy.valueOf(
              getConf().get("io.compression.codec.lzo.compressor",
                      LzoCompressor.CompressionStrategy.LZO1X_1.name()));
    int bufferSize =
      getConf().getInt("io.compression.codec.lzo.buffersize", 64*1024);
    return new LzopOutputStream(out, compressor, bufferSize, strategy);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in,
          Decompressor decompressor) throws IOException {
    // Ensure native-lzo library is loaded & initialized
    if (!isNativeLzoLoaded(getConf())) {
      throw new RuntimeException("native-lzo library not available");
    }
    return new LzopInputStream(in, decompressor,
            getConf().getInt("io.compression.codec.lzo.buffersize", 256 * 1024));
  }

  @Override
  public Decompressor createDecompressor() {
    if (!isNativeLzoLoaded(getConf())) {
      throw new RuntimeException("native-lzo library not available");
    }
    return new LzopDecompressor(getConf().getInt(
            "io.compression.codec.lzo.buffersize", 256 * 1024));
  }

  @Override
  public String getDefaultExtension() {
    return ".lzo";
  }
}
