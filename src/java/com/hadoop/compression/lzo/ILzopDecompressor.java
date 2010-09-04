package com.hadoop.compression.lzo;

/**
 * This interface only applies to lzop decompressor class implementations.
 * Used to decouple the code from LzopDecompressor so that it can be used
 * with any implementation
 * 
 * @author Fernando Ortiz
 * 
 * @see com.hadoop.compression.lzo.LzopDecompressor
 * @see com.hadoop.compression.lzo.jna.LzopDecompressor
 * @see com.hadoop.mapreduce.LzoSplitRecordReader
 *
 */
public abstract interface ILzopDecompressor
{
    public abstract int getCompressedChecksumsCount();

    public abstract int getDecompressedChecksumsCount();
}