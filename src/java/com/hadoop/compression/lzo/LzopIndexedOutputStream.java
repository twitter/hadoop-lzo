package com.hadoop.compression.lzo;

import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.Compressor;

import com.hadoop.compression.lzo.LzoCompressor.CompressionStrategy;

/**
 * An {@link LzopOutputStream} that also writes lzop block indices
 * to an output stream.
 */
public class LzopIndexedOutputStream extends LzopOutputStream {

  // This class can be easily folded into LzoOutputStream 

  DataOutputStream indexOut;
  CountingOutputStream cout;

  // keeps count of number of bytes written.
  private static class CountingOutputStream extends FilterOutputStream {
    public CountingOutputStream(OutputStream out) {
      super(out);
    }

    long bytesWritten = 0;

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
      if (len > 0)
        bytesWritten += len;
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
      bytesWritten++;
    }
  }

  public LzopIndexedOutputStream(OutputStream out,
                                 DataOutputStream indexOut,
                                 Compressor compressor,
                                 int bufferSize,
                                 CompressionStrategy strategy)
                                 throws IOException {
    super(new CountingOutputStream(out), compressor, bufferSize, strategy);

    this.indexOut = indexOut;
    this.cout = (CountingOutputStream) this.out;
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (indexOut != null) {
      indexOut.close();
      indexOut = null;
    }
  }

  @Override
  protected void compress() throws IOException {
    long start = cout.bytesWritten;
    super.compress();

    if ( cout.bytesWritten > start && indexOut != null ) {
      // new block is written. write the start pos
      indexOut.writeLong(start);
    }
  }

}
