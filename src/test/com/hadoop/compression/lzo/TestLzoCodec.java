package com.hadoop.compression.lzo;

import junit.framework.TestCase;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.conf.Configuration;

public class TestLzoCodec extends TestCase {

  /**
   * Simple test to make sure reinit can switch the compression strategy of the
   * same pooled codec instance
   **/
  public void testCodecPoolReinit() throws Exception {
    Configuration conf = new Configuration();
    CompressionCodec codec = ReflectionUtils.newInstance(
      LzoCodec.class, conf);

    // Put a codec in the pool
    Compressor c1 = CodecPool.getCompressor(codec);
    assertEquals(LzoCompressor.CompressionStrategy.LZO1X_1,
                 ((LzoCompressor)c1).getStrategy());
    CodecPool.returnCompressor(c1);

    // Set compression strategy
    LzoCodec.setCompressionStrategy(conf, LzoCompressor.CompressionStrategy.LZO1C_BEST_COMPRESSION);

    Compressor c2 = CodecPool.getCompressor(codec, conf);
    assertSame(c1, c2);

    assertEquals(LzoCompressor.CompressionStrategy.LZO1C_BEST_COMPRESSION,
                 ((LzoCompressor)c2).getStrategy());

  }
}