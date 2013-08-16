/*
 * This file is part of Hadoop-Gpl-Compression.
 * 
 * Hadoop-Gpl-Compression is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * 
 * Hadoop-Gpl-Compression is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * Hadoop-Gpl-Compression. If not, see <http://www.gnu.org/licenses/>.
 */
 
package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is a bridging class whose sole purpose is to provide backward
 * compatibility of applications that historically depend on 
 * {@link org.apache.hadoop.io.compress.LzoCodec} (such as SequenceFile).
 * 
 * The class is marked as @deprecated and should not be used explicitly in the
 * future.
 * 
 * A warning message will be generated when a user wants to use this class to
 * generate LZO compressed data. Not the case for decompression because this is
 * the legitimate backward compatibility usage.
 */
@Deprecated
public class LzoCodec extends com.hadoop.compression.lzo.LzoCodec {
  private static final Log LOG = LogFactory.getLog(LzoCodec.class);

  static final String oahLzoCodec = LzoCodec.class.getName();
  static final String chclLzoCodec =
    com.hadoop.compression.lzo.LzoCodec.class.getName();
  static boolean warned = false;

  static {
    LOG.info("Bridging " + oahLzoCodec + " to " + chclLzoCodec + ".");
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out,
      Compressor compressor) throws IOException {
    if (!warned) {
      LOG.warn(oahLzoCodec + " is deprecated. You should use " + chclLzoCodec
          + " instead to generate LZO compressed data.");
      warned = true;
    }
    return super.createOutputStream(out, compressor);
  }
}
