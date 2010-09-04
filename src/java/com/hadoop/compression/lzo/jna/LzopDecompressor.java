package com.hadoop.compression.lzo.jna;

import java.io.IOException;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.zip.Checksum;

import com.hadoop.compression.lzo.CChecksum;
import com.hadoop.compression.lzo.DChecksum;
import com.hadoop.compression.lzo.ILzopDecompressor;

public class LzopDecompressor extends LzoDecompressor implements ILzopDecompressor
{
    private final EnumMap<DChecksum, Checksum> chkDMap = new EnumMap<DChecksum, Checksum>(
                                                               DChecksum.class);
    private final EnumMap<CChecksum, Checksum> chkCMap = new EnumMap<CChecksum, Checksum>(
                                                               CChecksum.class);

    public LzopDecompressor()
    {
        super(LzoDecompressor.CompressionStrategy.LZO1X, 256 * 1024);
    }

    /**
     * Create an LzoDecompressor with LZO1X strategy (the only lzo algorithm
     * supported by lzop).
     */
    public LzopDecompressor(int bufferSize)
    {
        super(LzoDecompressor.CompressionStrategy.LZO1X_SAFE, bufferSize);
    }

    /**
     * Given a set of decompressed and compressed checksums,
     */
    public void initHeaderFlags(EnumSet<DChecksum> dflags,
            EnumSet<CChecksum> cflags)
    {
        try
        {
            for (final DChecksum flag : dflags)
            {
                this.chkDMap.put(flag, flag.getChecksumClass().newInstance());
            }
            for (final CChecksum flag : cflags)
            {
                this.chkCMap.put(flag, flag.getChecksumClass().newInstance());
            }
        }
        catch (final InstantiationException e)
        {
            throw new RuntimeException("Internal error", e);
        }
        catch (final IllegalAccessException e)
        {
            throw new RuntimeException("Internal error", e);
        }
    }

    /**
     * Get the number of checksum implementations the current lzo file uses.
     * 
     * @return Number of checksum implementations in use.
     */
    public int getChecksumsCount()
    {
        return this.getCompressedChecksumsCount()
                + this.getDecompressedChecksumsCount();
    }

    /**
     * Get the number of compressed checksum implementations the current lzo
     * file uses.
     * 
     * @return Number of compressed checksum implementations in use.
     */
    @Override
    public int getCompressedChecksumsCount()
    {
        return this.chkCMap.size();
    }

    /**
     * Get the number of decompressed checksum implementations the current lzo
     * file uses.
     * 
     * @return Number of decompressed checksum implementations in use.
     */
    @Override
    public int getDecompressedChecksumsCount()
    {
        return this.chkDMap.size();
    }

    /**
     * Reset all checksums registered for this decompressor instance.
     */
    public synchronized void resetChecksum()
    {
        for (final Checksum chk : this.chkDMap.values())
        {
            chk.reset();
        }
        for (final Checksum chk : this.chkCMap.values())
        {
            chk.reset();
        }
    }

    /**
     * Given a checksum type, verify its value against that observed in
     * decompressed data.
     */
    public synchronized boolean verifyDChecksum(DChecksum typ, int checksum)
    {
        return (checksum == (int) this.chkDMap.get(typ).getValue());
    }

    /**
     * Given a checksum type, verity its value against that observed in
     * compressed data.
     */
    public synchronized boolean verifyCChecksum(CChecksum typ, int checksum)
    {
        return (checksum == (int) this.chkCMap.get(typ).getValue());
    }

    @Override
    public synchronized void setInput(byte[] b, int off, int len)
    {
        if (!this.isCurrentBlockUncompressed())
        {
            for (final Checksum chk : this.chkCMap.values())
            {
                chk.update(b, off, len);
            }
        }
        super.setInput(b, off, len);
    }

    @Override
    public synchronized boolean needsDictionary()
    {
        return super.needsDictionary();
    }

    @Override
    public synchronized int decompress(byte[] b, int off, int len)
            throws IOException
    {
        final int ret = super.decompress(b, off, len);
        if (ret > 0)
        {
            for (final Checksum chk : this.chkDMap.values())
            {
                chk.update(b, off, len);
            }
        }
        return ret;
    }
}
