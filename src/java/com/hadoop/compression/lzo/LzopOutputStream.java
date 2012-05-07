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

import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Adler32;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.Compressor;

public class LzopOutputStream extends CompressorStream {

  final int MAX_INPUT_SIZE;
  protected DataOutputStream indexOut;
  private CountingOutputStream cout;

  /**
   * Write an lzop-compatible header to the OutputStream provided.
   */
  protected static void writeLzopHeader(OutputStream out,
          LzoCompressor.CompressionStrategy strategy) throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    try {
      dob.writeShort(LzopCodec.LZOP_VERSION);
      dob.writeShort(LzoCompressor.LZO_LIBRARY_VERSION);
      dob.writeShort(LzopCodec.LZOP_COMPAT_VERSION);
      switch (strategy) {
      case LZO1X_1:
        dob.writeByte(1);
        dob.writeByte(5);
        break;
      case LZO1X_15:
        dob.writeByte(2);
        dob.writeByte(1);
        break;
      case LZO1X_999:
        dob.writeByte(3);
        dob.writeByte(9);
        break;
      default:
        throw new IOException("Incompatible lzop strategy: " + strategy);
      }
      dob.writeInt(0);                                    // all flags 0
      dob.writeInt(0x81A4);                               // mode
      dob.writeInt((int)(System.currentTimeMillis() / 1000)); // mtime
      dob.writeInt(0);                                    // gmtdiff ignored
      dob.writeByte(0);                                   // no filename
      Adler32 headerChecksum = new Adler32();
      headerChecksum.update(dob.getData(), 0, dob.getLength());
      int hc = (int)headerChecksum.getValue();
      dob.writeInt(hc);
      out.write(LzopCodec.LZO_MAGIC);
      out.write(dob.getData(), 0, dob.getLength());
    } finally {
      dob.close();
    }
  }

  public LzopOutputStream(OutputStream out, Compressor compressor,
          int bufferSize, LzoCompressor.CompressionStrategy strategy)
  throws IOException {
    this(out, null, compressor, bufferSize, strategy);
  }

  public LzopOutputStream(OutputStream out, DataOutputStream indexOut,
      Compressor compressor, int bufferSize,
      LzoCompressor.CompressionStrategy strategy)
      throws IOException {
    super(new CountingOutputStream(out), compressor, bufferSize);

    this.cout = (CountingOutputStream) this.out;
    this.indexOut = indexOut;
    int overhead = strategy.name().contains("LZO1") ?
      (bufferSize >> 4) + 64 + 3 : (bufferSize >> 3) + 128 + 3;
    MAX_INPUT_SIZE = bufferSize - overhead;

    writeLzopHeader(this.out, strategy);
  }

  /**
   * Close the underlying stream and write a null word to the output stream.
   */
  @Override
  public void close() throws IOException {
    if (!closed) {
      finish();
      out.write(new byte[]{ 0, 0, 0, 0 });
      out.close();
      if (indexOut != null) {
        indexOut.close();
      }
      closed = true;
      //return the compressor to the pool for later reuse;
      //the returnCompressor handles nulls.
      CodecPool.returnCompressor(compressor);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // TODO: LzopOutputStream used to inherit from BlockCompressorStream
    // but had a bug due to this inheritance chain. In order to fix the
    // bug we pulled down the implementation of the superclass, which
    // is overly general. Thus this function is not quite as succint
    // as it could be, now that it's LZOP-specific.
    // See: http://github.com/toddlipcon/hadoop-lzo/commit/5fe6dd4736a73fa33b86656ce8aeb011e7f2046c

    // Sanity checks
    if (compressor.finished()) {
      throw new IOException("write beyond end of stream");
    }
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
               ((off + len) > b.length)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    long limlen = compressor.getBytesRead();
    if (len + limlen > MAX_INPUT_SIZE && limlen > 0) {
      // Adding this segment would exceed the maximum size.
      // Flush data if we have it.
      finish();
      compressor.reset();
    }

    if (len > MAX_INPUT_SIZE) {
      // The data we're given exceeds the maximum size. Any data
      // we had have been flushed, so we write out this chunk in segments
      // not exceeding the maximum size until it is exhausted.
      do {
        int bufLen = Math.min(len, MAX_INPUT_SIZE);

        compressor.setInput(b, off, bufLen);
        finish();
        compressor.reset();
        off += bufLen;
        len -= bufLen;
      } while (len > 0);
      return;
    }

    // Give data to the compressor
    compressor.setInput(b, off, len);
    if (!compressor.needsInput()) {
      // compressor buffer size might be smaller than the maximum
      // size, so we permit it to flush if required.
      do {
        compress();
      } while (!compressor.needsInput());
    }
  }

  @Override
  public void finish() throws IOException {
    if (!compressor.finished()) {
      compressor.finish();
      while (!compressor.finished()) {
        compress();
      }
    }
  }
  @Override
  protected void compress() throws IOException {
    int len = compressor.compress(buffer, 0, buffer.length);
    if (len > 0) {
      // new lzo block. write current position to index file.
      if (indexOut != null) {
        indexOut.writeLong(cout.bytesWritten);
      }

      rawWriteInt((int)compressor.getBytesRead());

      // If the compressed buffer is actually larger than the uncompressed buffer,
      // the LZO specification says that we should write the uncompressed bytes rather
      // than the compressed bytes.  The decompressor understands this because both sizes
      // get written to the stream.
      if (compressor.getBytesRead() <= compressor.getBytesWritten()) {
        // Compression actually increased the size of the buffer, so write the uncompressed bytes.
        byte[] uncompressed = ((LzoCompressor)compressor).uncompressedBytes();
        rawWriteInt(uncompressed.length);
        out.write(uncompressed, 0, uncompressed.length);
      } else {
        // Write out the compressed chunk.
        rawWriteInt(len);
        out.write(buffer, 0, len);
      }
    }
  }

  private void rawWriteInt(int v) throws IOException {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v >>>  0) & 0xFF);
  }

  /* keeps count of number of bytes written. */
  private static class CountingOutputStream extends FilterOutputStream {
    public CountingOutputStream(OutputStream out) {
      super(out);
    }

    long bytesWritten = 0;

    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
      bytesWritten += len;
    }

    public void write(int b) throws IOException {
      out.write(b);
      bytesWritten++;
    }
  }
}
