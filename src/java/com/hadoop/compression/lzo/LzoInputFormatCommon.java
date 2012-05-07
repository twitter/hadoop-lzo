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

import org.apache.hadoop.conf.Configuration;

import com.hadoop.compression.lzo.LzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;

/**
 * Utilities used by the two LzoInputFormat implementations.
 */
public abstract class LzoInputFormatCommon {
  /**
   * The boolean property <code>lzo.text.input.format.ignore.nonlzo</code> tells
   * the LZO text input format whether it should silently ignore non-LZO input
   * files. When the property is true (which is the default), non-LZO files will
   * be silently ignored. When the property is false, non-LZO files will be
   * processed using the standard TextInputFormat.
   */
  public static final String IGNORE_NONLZO_KEY = "lzo.text.input.format.ignore.nonlzo";
  /**
   * Default value of the <code>lzo.text.input.format.ignore.nonlzo</code>
   * property.
   */
  public static final boolean DEFAULT_IGNORE_NONLZO = true;
  /**
   * Full extension for LZO index files (".lzo.index").
   */
  public static final String FULL_LZO_INDEX_SUFFIX =
    LzopCodec.DEFAULT_LZO_EXTENSION + LzoIndex.LZO_INDEX_SUFFIX;

  /**
   * @param conf the Configuration object
   * @return the value of the <code>lzo.text.input.format.ignore.nonlzo</code>
   *         property in <code>conf</code>, or <code>DEFAULT_IGNORE_NONLZO</code>
   *         if the property is not set.
   */
  public static boolean getIgnoreNonLzoProperty(Configuration conf) {
    return conf.getBoolean(IGNORE_NONLZO_KEY, DEFAULT_IGNORE_NONLZO);
  }

  /**
   * Checks if the given filename ends in ".lzo".
   *
   * @param filename filename to check.
   * @return true if the filename ends in ".lzo"
   */
  public static boolean isLzoFile(String filename) {
    return filename.endsWith(LzopCodec.DEFAULT_LZO_EXTENSION);
  }

  /**
   * Checks if the given filename ends in ".lzo.index".
   *
   * @param filename filename to check.
   * @return true if the filename ends in ".lzo.index"
   */
  public static boolean isLzoIndexFile(String filename) {
    return filename.endsWith(FULL_LZO_INDEX_SUFFIX);
  }
}
