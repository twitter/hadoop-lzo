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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GPLNativeCodeLoader {
  public static final String LIBRARY_NAME = "gplcompression";
  /**
   * The system property that causes hadoop-lzo to ignore the embedded native
   * library and load it from the normal library path instead. It is false by
   * default (i.e. it loads the embedded native library if found over the
   * library path).
   */
  public static final String USE_BINARIES_ON_LIB_PATH =
      "com.hadoop.compression.lzo.use.libpath";

  private static final Log LOG = LogFactory.getLog(GPLNativeCodeLoader.class);
  private static boolean nativeLibraryLoaded = false;

  static {
    try {
      //try to load the lib
      if (!useBinariesOnLibPath()) {
        File unpackedFile = unpackBinaries();
        if (unpackedFile != null) { // the file was successfully unpacked
          String path = unpackedFile.getAbsolutePath();
          System.load(path);
          LOG.info("Loaded native gpl library from the embedded binaries");
        } else { // fall back
          System.loadLibrary(LIBRARY_NAME);
          LOG.info("Loaded native gpl library from the library path");
        }
      } else {
        System.loadLibrary(LIBRARY_NAME);
        LOG.info("Loaded native gpl library from the library path");
      }
      nativeLibraryLoaded = true;
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

  private static boolean useBinariesOnLibPath() {
    return Boolean.getBoolean(USE_BINARIES_ON_LIB_PATH);
  }

  /**
   * Locates the native library in the jar (loadble by the classloader really),
   * unpacks it in a temp location, and returns that file. If the native library
   * is not found by the classloader, returns null.
   */
  private static File unpackBinaries() {
    // locate the binaries inside the jar
    String fileName = System.mapLibraryName(LIBRARY_NAME);
    String directory = getDirectoryLocation();
    // use the current defining classloader to load the resource
    InputStream is =
        GPLNativeCodeLoader.class.getResourceAsStream(directory + "/" + fileName);
    if (is == null) {
      // specific to mac
      // on mac the filename can be either .dylib or .jnilib: try again with the
      // alternate name
      if (getOsName().contains("Mac")) {
        if (fileName.endsWith(".dylib")) {
          fileName = fileName.replace(".dylib", ".jnilib");
        } else if (fileName.endsWith(".jnilib")) {
          fileName = fileName.replace(".jnilib", ".dylib");
        }
        is = GPLNativeCodeLoader.class.getResourceAsStream(directory + "/" + fileName);
      }
      // the OS-specific library was not found: fall back on the library path
      if (is == null) {
        return null;
      }
    }

    // write the file
    byte[] buffer = new byte[8192];
    OutputStream os = null;
    try {
      // prepare the unpacked file location
      File unpackedFile = File.createTempFile("unpacked-", "-" + fileName);
      // ensure the file gets cleaned up
      unpackedFile.deleteOnExit();

      os = new FileOutputStream(unpackedFile);
      int read = 0;
      while ((read = is.read(buffer)) != -1) {
        os.write(buffer, 0, read);
      }

      // set the execution permission
      unpackedFile.setExecutable(true, false);
      LOG.debug("temporary unpacked path: " + unpackedFile);
      // return the file
      return unpackedFile;
    } catch (IOException e) {
      LOG.error("could not unpack the binaries", e);
      return null;
    } finally {
      try { is.close(); } catch (IOException ignore) {}
      if (os != null) {
        try { os.close(); } catch (IOException ignore) {}
      }
    }
  }

  private static String getDirectoryLocation() {
    String osName = getOsName().replace(' ', '_');
    boolean windows = osName.toLowerCase().contains("windows");
    if (!windows) {
      String location = "/native/" + osName + "-" + System.getProperty("os.arch") + "-" +
          System.getProperty("sun.arch.data.model") + "/lib";
      LOG.debug("location: " + location);
      return location;
    } else {
      String location = "/native/" + System.getenv("OS") + "-" + System.getenv("PLATFORM") + "/lib";
      LOG.debug("location: " + location);
      return location;
    }
  }

  private static String getOsName() {
    return System.getProperty("os.name");
  }
}
