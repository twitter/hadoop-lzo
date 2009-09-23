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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GPLNativeCodeLoader {

  private static final Log LOG = LogFactory.getLog(GPLNativeCodeLoader.class);
  private static boolean nativeLibraryLoaded = false;

  static {
    try {
      //try to load the lib
      System.loadLibrary("gplcompression");
      nativeLibraryLoaded = true;
      LOG.info("Loaded native gpl library");
    } catch (Throwable t) {
      LOG.error("Could not load native gpl library", t);
      nativeLibraryLoaded = false;
    }
  }

  /**
   * Are the native gpl libraries loaded? 
   * @return true if loaded, otherwise false
   */
  public static boolean isNativeCodeLoaded() {
    return nativeLibraryLoaded;
  }

}
