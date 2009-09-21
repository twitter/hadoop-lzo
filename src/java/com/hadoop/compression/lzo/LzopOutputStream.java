package com.hadoop.compression.lzo;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Adler32;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.Compressor;

public class LzopOutputStream extends BlockCompressorStream {

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
    super(out, compressor, bufferSize, strategy.name().contains("LZO1")
            ? (bufferSize >> 4) + 64 + 3
                    : (bufferSize >> 3) + 128 + 3);
    writeLzopHeader(out, strategy);
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
      closed = true;
    }
  }

}