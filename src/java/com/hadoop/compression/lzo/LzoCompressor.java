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
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

/**
 * A {@link Compressor} based on the lzo algorithm.
 * http://www.oberhumer.com/opensource/lzo/
 * 
 */
class LzoCompressor implements Compressor {
  private static final Log LOG = 
    LogFactory.getLog(LzoCompressor.class.getName());

  // HACK - Use this as a global lock in the JNI layer
  @SuppressWarnings({ "unchecked", "unused" })
  private static Class clazz = LzoDecompressor.class;

  private int directBufferSize;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private ByteBuffer uncompressedDirectBuf = null;
  private int uncompressedDirectBufLen = 0;
  private ByteBuffer compressedDirectBuf = null;
  private boolean finish, finished;

  private long bytesRead = 0L;
  private long bytesWritten = 0L;

  private CompressionStrategy strategy; // The lzo compression algorithm.
  @SuppressWarnings("unused")
  private long lzoCompressor = 0;       // The actual lzo compression function.
  private int workingMemoryBufLen = 0;  // The length of 'working memory' buf.
  @SuppressWarnings("unused")
  private ByteBuffer workingMemoryBuf;      // The 'working memory' for lzo.
  private int lzoCompressionLevel;

  /**
   * Used when the user doesn't specify a configuration. We cache a single
   * one statically, since loading the defaults is expensive.
   */
  private static Configuration defaultConfiguration =
    new Configuration();

  /**
   * The compression algorithm for lzo library.
   */
  public static enum CompressionStrategy {
    /**
     * lzo1 algorithms.
     */
    LZO1 (0),
    LZO1_99 (1),

    /**
     * lzo1a algorithms.
     */
    LZO1A (2),
    LZO1A_99 (3),

    /**
     * lzo1b algorithms.
     */
    LZO1B (4),
    LZO1B_BEST_COMPRESSION(5),
    LZO1B_BEST_SPEED(6),
    LZO1B_1 (7),
    LZO1B_2 (8),
    LZO1B_3 (9),
    LZO1B_4 (10),
    LZO1B_5 (11),
    LZO1B_6 (12),
    LZO1B_7 (13),
    LZO1B_8 (14),
    LZO1B_9 (15),
    LZO1B_99 (16),
    LZO1B_999 (17),

    /**
     * lzo1c algorithms.
     */
    LZO1C (18),
    LZO1C_BEST_COMPRESSION(19),
    LZO1C_BEST_SPEED(20),
    LZO1C_1 (21),
    LZO1C_2 (22),
    LZO1C_3 (23),
    LZO1C_4 (24),
    LZO1C_5 (25),
    LZO1C_6 (26),
    LZO1C_7 (27),
    LZO1C_8 (28),
    LZO1C_9 (29),
    LZO1C_99 (30),
    LZO1C_999 (31),

    /**
     * lzo1f algorithms.
     */
    LZO1F_1 (32),
    LZO1F_999 (33),

    /**
     * lzo1x algorithms.
     */
    LZO1X_1 (34),
    LZO1X_11 (35),
    LZO1X_12 (36),
    LZO1X_15 (37),
    LZO1X_999 (38),

    /**
     * lzo1y algorithms.
     */
    LZO1Y_1 (39),
    LZO1Y_999 (40),

    /**
     * lzo1z algorithms.
     */
    LZO1Z_999 (41),

    /**
     * lzo2a algorithms.
     */
    LZO2A_999 (42);

    private final int compressor;

    private CompressionStrategy(int compressor) {
      this.compressor = compressor;
    }

    int getCompressor() {
      return compressor;
    }
  }; // CompressionStrategy

  private static boolean nativeLzoLoaded;
  public static final int LZO_LIBRARY_VERSION;

  static {
    if (GPLNativeCodeLoader.isNativeCodeLoaded()) {
      // Initialize the native library
      try {
        initIDs();
        nativeLzoLoaded = true;
      } catch (Throwable t) {
        // Ignore failure to load/initialize native-lzo
        LOG.warn(t.toString());
        nativeLzoLoaded = false;
      }
      LZO_LIBRARY_VERSION = (nativeLzoLoaded) ? 0xFFFF & getLzoLibraryVersion()
          : -1;
    } else {
      LOG.error("Cannot load " + LzoCompressor.class.getName() + 
      " without native-hadoop library!");
      nativeLzoLoaded = false;
      LZO_LIBRARY_VERSION = -1;
    }
  }

  /**
   * Check if lzo compressors are loaded and initialized.
   * 
   * @return <code>true</code> if lzo compressors are loaded & initialized,
   *         else <code>false</code> 
   */
  public static boolean isNativeLzoLoaded() {
    return nativeLzoLoaded;
  }

  public LzoCompressor(Configuration conf) {
    reinit(conf);
  }

  /**
   * Reinitialize from a configuration, possibly changing compression codec
   */
  //@Override (this method isn't in vanilla 0.20.2, but is in CDH3b3 and YDH)
  public void reinit(Configuration conf) {
    // It's possible conf is null in the case that the compressor was got from a pool,
    // and the new user of the codec doesn't specify a particular configuration
    // to CodecPool.getCompressor(). So we use the defaults.
    if (conf == null) {
      conf = defaultConfiguration;
    }
    LzoCompressor.CompressionStrategy strategy = LzoCodec.getCompressionStrategy(conf);
    int compressionLevel = LzoCodec.getCompressionLevel(conf);
    int bufferSize = LzoCodec.getBufferSize(conf);

    init(strategy, compressionLevel, bufferSize);
  }

  /** 
   * Creates a new compressor using the specified {@link CompressionStrategy}.
   * 
   * @param strategy lzo compression algorithm to use
   * @param directBufferSize size of the direct buffer to be used.
   */
  public LzoCompressor(CompressionStrategy strategy, int directBufferSize) {
    init(strategy, LzoCodec.UNDEFINED_COMPRESSION_LEVEL, directBufferSize);
  }

  /**
   * Reallocates a direct byte buffer by freeing the old one and allocating
   * a new one, unless the size is the same, in which case it is simply
   * cleared and returned.
   *
   * NOTE: this uses unsafe APIs to manually free memory - if anyone else
   * has a reference to the 'buf' parameter they will likely read random
   * data or cause a segfault by accessing it.
   */
  private ByteBuffer realloc(ByteBuffer buf, int newSize) {
    if (buf != null) {
      if (buf.capacity() == newSize) {
        // Can use existing buffer
        buf.clear();
        return buf;
      }
      try {
        // Manually free the old buffer using undocumented unsafe APIs.
        // If this fails, we'll drop the reference and hope GC finds it
        // eventually.
        Object cleaner = buf.getClass().getMethod("cleaner").invoke(buf);
        cleaner.getClass().getMethod("clean").invoke(cleaner);
      } catch (Exception e) {
        // Perhaps a non-sun-derived JVM - contributions welcome
        LOG.warn("Couldn't realloc bytebuffer", e);
      }
    }
    return ByteBuffer.allocateDirect(newSize);
  }

  private void init(CompressionStrategy strategy, int compressionLevel, int directBufferSize) {
    this.strategy = strategy;
    this.lzoCompressionLevel = compressionLevel;
    this.directBufferSize = directBufferSize;

    uncompressedDirectBuf = realloc(uncompressedDirectBuf, directBufferSize);
    compressedDirectBuf = realloc(compressedDirectBuf, directBufferSize);
    compressedDirectBuf.position(directBufferSize);
    reset();

    /**
     * Initialize {@link #lzoCompress} and {@link #workingMemoryBufLen}
     */
    init(this.strategy.getCompressor());
    workingMemoryBuf = realloc(workingMemoryBuf, workingMemoryBufLen);
  }

  /**
   * Creates a new compressor with the default lzo1x_1 compression.
   */
  public LzoCompressor() {
    this(CompressionStrategy.LZO1X_1, 64*1024);
  }

  public synchronized void setInput(byte[] b, int off, int len) {
    if (b== null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    finished = false;

    if (len > uncompressedDirectBuf.remaining()) {
      // save data; now !needsInput
      this.userBuf = b;
      this.userBufOff = off;
      this.userBufLen = len;
    } else {
      ((ByteBuffer)uncompressedDirectBuf).put(b, off, len);
      uncompressedDirectBufLen = uncompressedDirectBuf.position();
    }
    bytesRead += len;
  }

  /**
   * If a write would exceed the capacity of the direct buffers, it is set
   * aside to be loaded by this function while the compressed data are
   * consumed.
   */
  synchronized void setInputFromSavedData() {
    if (0 >= userBufLen) {
      return;
    }
    finished = false;

    uncompressedDirectBufLen = Math.min(userBufLen, directBufferSize);
    ((ByteBuffer)uncompressedDirectBuf).put(userBuf, userBufOff,
        uncompressedDirectBufLen);

    // Note how much data is being fed to lzo
    userBufOff += uncompressedDirectBufLen;
    userBufLen -= uncompressedDirectBufLen;
  }

  public synchronized void setDictionary(byte[] b, int off, int len) {
    // nop
  }

  /** {@inheritDoc} */
  public boolean needsInput() {
    return !(compressedDirectBuf.remaining() > 0
        || uncompressedDirectBuf.remaining() == 0
        || userBufLen > 0);
  }

  public synchronized void finish() {
    finish = true;
  }

  public synchronized boolean finished() {
    // Check if 'lzo' says its 'finished' and
    // all compressed data has been consumed
    return (finish && finished && compressedDirectBuf.remaining() == 0); 
  }

  public synchronized int compress(byte[] b, int off, int len) 
  throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    // Check if there is compressed data
    int n = compressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((ByteBuffer)compressedDirectBuf).get(b, off, n);
      bytesWritten += n;
      return n;
    }

    // Re-initialize the lzo's output direct-buffer
    compressedDirectBuf.clear();
    compressedDirectBuf.limit(0);
    if (0 == uncompressedDirectBuf.position()) {
      // No compressed data, so we should have !needsInput or !finished
      setInputFromSavedData();
      if (0 == uncompressedDirectBuf.position()) {
        // Called without data; write nothing
        finished = true;
        return 0;
      }
    }

    // Compress data
    n = compressBytesDirect(strategy.getCompressor());
    compressedDirectBuf.limit(n);
    uncompressedDirectBuf.clear(); // lzo consumes all buffer input

    // Set 'finished' if lzo has consumed all user-data
    if (0 == userBufLen) {
      finished = true;
    }

    // Get atmost 'len' bytes
    n = Math.min(n, len);
    bytesWritten += n;
    ((ByteBuffer)compressedDirectBuf).get(b, off, n);

    return n;
  }

  public synchronized void reset() {
    finish = false;
    finished = false;
    uncompressedDirectBuf.clear();
    uncompressedDirectBufLen = 0;
    compressedDirectBuf.clear();
    compressedDirectBuf.limit(0);
    userBufOff = userBufLen = 0;
    bytesRead = bytesWritten = 0L;
  }

  /**
   * Return number of bytes given to this compressor since last reset.
   */
  public synchronized long getBytesRead() {
    return bytesRead;
  }

  /**
   * Return number of bytes consumed by callers of compress since last reset.
   */
  public synchronized long getBytesWritten() {
    return bytesWritten;
  }
  
  /**
   * Return the uncompressed byte buffer contents, for use when the compressed block
   * would be larger than the uncompressed block, because the LZO spec dictates that
   * the uncompressed bytes are written to the file in this case.
   */
  public byte[] uncompressedBytes() {
    byte[] b = new byte[(int)bytesRead];
    ((ByteBuffer)uncompressedDirectBuf).get(b);
    return b;
  }

  /**
   * Used only by tests.
   */
  long getDirectBufferSize() {
    return directBufferSize;
  }
  
  /**
   * Noop.
   */
  public synchronized void end() {
    // nop
  }

  /** used for tests */
  CompressionStrategy getStrategy() {
    return strategy;
  }

  private native static void initIDs();
  private native static int getLzoLibraryVersion();
  private native void init(int compressor);
  private native int compressBytesDirect(int compressor);
}
