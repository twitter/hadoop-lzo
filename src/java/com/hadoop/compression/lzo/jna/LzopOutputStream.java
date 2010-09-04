package com.hadoop.compression.lzo.jna;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Adler32;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;

public class LzopOutputStream extends CompressorStream
{
    private final int MAX_INPUT_SIZE;

    public LzopOutputStream(OutputStream out, Compressor compressor,
            int bufferSize, LzoCompressor.CompressionStrategy strategy)
            throws IOException
    {
        super(out, compressor, bufferSize);
        this.compressor = compressor;
        final int overhead = strategy.name().contains("LZO1") ? (bufferSize >> 4) + 64 + 3
                : (bufferSize >> 3) + 128 + 3;
        this.MAX_INPUT_SIZE = bufferSize - overhead;
        this.writeLzopHeader(out, strategy);
    }

    /**
     * Write an lzop-compatible header to the OutputStream provided.
     */
    public void writeLzopHeader(OutputStream out,
            LzoCompressor.CompressionStrategy strategy) throws IOException
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        try
        {
            dob.writeShort(LzopCodec.LZOP_VERSION);
            dob.writeShort(LzoCompressor.LZO_LIBRARY_VERSION);
            dob.writeShort(LzopCodec.LZOP_COMPAT_VERSION);
            switch (strategy)
            {
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
                    throw new IOException("Incompatible lzop strategy: "
                            + strategy);
            }
            dob.writeInt(0); // all flags 0
            dob.writeInt(0x81A4); // mode
            dob.writeInt((int) (System.currentTimeMillis() / 1000)); // mtime
            dob.writeInt(0); // gmtdiff ignored
            dob.writeByte(0); // no filename
            Adler32 headerChecksum = new Adler32();
            headerChecksum.update(dob.getData(), 0, dob.getLength());
            int hc = (int) headerChecksum.getValue();
            dob.writeInt(hc);
            out.write(LzopCodec.LZO_MAGIC);
            out.write(dob.getData(), 0, dob.getLength());
        }
        finally
        {
            dob.close();
        }
    }

    /**
     * Close the underlying stream and write a null word to the output stream.
     */
    @Override
    public void close() throws IOException
    {
        if (!this.closed)
        {
            this.finish();
            this.out.write(new byte[]
            { 0, 0, 0, 0 });
            this.out.close();
            this.closed = true;
        }
    }

    @Override
    public void flush() throws IOException
    {
        this.out.flush();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        if (this.compressor.finished())
        {
            throw new IOException("write beyond end of stream");
        }
        if (b == null)
        {
            throw new NullPointerException();
        }
        else if ((off < 0) || (off > b.length) || (len < 0)
                || ((off + len) > b.length))
        {
            throw new ArrayIndexOutOfBoundsException();
        }
        else if (len == 0)
        {
            return;
        }

        final long limlen = this.compressor.getBytesRead();
        if (len + limlen > this.MAX_INPUT_SIZE && limlen > 0)
        {
            this.finish();
            this.compressor.reset();
        }

        if (len > this.MAX_INPUT_SIZE)
        {
            do
            {
                final int bufLen = Math.min(len, this.MAX_INPUT_SIZE);
                this.compressor.setInput(b, off, bufLen);
                this.finish();
                this.compressor.reset();
                off += bufLen;
                len -= bufLen;
            }
            while (len > 0);
            return;
        }

        this.compressor.setInput(b, off, len);
        if (!this.compressor.needsInput())
        {
            do
            {
                this.compress();
            }
            while (!this.compressor.needsInput());
        }
    }

    @Override
    public void finish() throws IOException
    {
        if (!this.compressor.finished())
        {
            this.compressor.finish();
            while (!this.compressor.finished())
            {
                this.compress();
            }
        }
    }

    @Override
    protected void compress() throws IOException
    {
        int len = this.compressor.compress(this.buffer, 0, this.buffer.length);
        if (len > 0)
        {
            this.rawWriteInt((int) this.compressor.getBytesRead());
            if (this.compressor.getBytesRead() < this.compressor
                    .getBytesWritten())
            {
                byte[] uncompressed = ((LzoCompressor) this.compressor)
                        .uncompressedBytes();
                this.rawWriteInt(uncompressed.length);
                out.write(uncompressed, 0, uncompressed.length);
            }
            else
            {
                this.rawWriteInt(len);
                out.write(this.buffer, 0, this.buffer.length);
            }
        }
    }

    private void rawWriteInt(int v) throws IOException
    {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write((v >>> 0) & 0xFF);
    }
}
