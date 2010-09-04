package com.hadoop.compression.lzo.jna;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import com.hadoop.compression.lzo.LzoDefaults;

public class LzopCodec extends LzoCodec
{
    /** 9 bytes at the top of every lzo file */
    public static final byte[] LZO_MAGIC = new byte[] {
      -119, 'L', 'Z', 'O', 0, '\r', '\n', '\032', '\n' };
    /** Version of lzop this emulates */
    public static final int LZOP_VERSION = 0x1010;
    /** Latest verion of lzop this should be compatible with */
    public static final int LZOP_COMPAT_VERSION = 0x0940;

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out)
            throws IOException
    {
        return createOutputStream(out, createCompressor());
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out,
            Compressor compressor) throws IOException
    {
        if (!isNativeLzoLoaded(getConf()))
        {
            throw new RuntimeException("native-lzo library not available");
        }
        LzoCompressor.CompressionStrategy strategy = LzoCompressor.CompressionStrategy
                .valueOf(getConf().get(LZO_COMPRESSOR_KEY,
                        LzoCompressor.CompressionStrategy.LZO1X_1.name()));
        int bufferSize = getConf().getInt(LZO_BUFFER_SIZE_KEY,
                DEFAULT_LZO_BUFFER_SIZE);
        return new LzopOutputStream(out, compressor, bufferSize, strategy);
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in,
            Decompressor decompressor) throws IOException
    {
        // Ensure native-lzo library is loaded & initialized
        if (!isNativeLzoLoaded(getConf()))
        {
            throw new RuntimeException("native-lzo library not available");
        }
        return new LzopInputStream(in, decompressor, getConf().getInt(
                LZO_BUFFER_SIZE_KEY, DEFAULT_LZO_BUFFER_SIZE));
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in)
            throws IOException
    {
        return createInputStream(in, createDecompressor());
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType()
    {
        // Ensure native-lzo library is loaded & initialized
        if (!isNativeLzoLoaded(getConf()))
        {
            throw new RuntimeException("native-lzo library not available");
        }
        return LzopDecompressor.class;
    }

    @Override
    public Decompressor createDecompressor()
    {
        if (!isNativeLzoLoaded(getConf()))
        {
            throw new RuntimeException("native-lzo library not available");
        }
        return new LzopDecompressor(getConf().getInt(LZO_BUFFER_SIZE_KEY,
                DEFAULT_LZO_BUFFER_SIZE));
    }

    @Override
    public String getDefaultExtension()
    {
        return LzoDefaults.getLzopDefaultExtension();
    }
}
