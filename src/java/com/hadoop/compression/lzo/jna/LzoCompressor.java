package com.hadoop.compression.lzo.jna;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.compress.Compressor;

import com.hadoop.compression.lzo.jna.LzoJna.CompressorInfo;
import com.sun.jna.Function;
import com.sun.jna.Memory;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.NativeLongByReference;

public class LzoCompressor implements Compressor
{
    private int                 directBufferSize;
    private byte[]              userBuf                  = null;
    private int                 userBufOff               = 0;
    private int                 userBufLen               = 0;
    private Buffer              uncompressedDirectBuf    = null;
    private int                 uncompressedDirectBufLen = 0;
    private Buffer              compressedDirectBuf      = null;
    private int                 workingMemoryBufLen      = 0;

    private boolean             finish;
    private boolean             finished;
    private long                bytesRead                = 0L;
    private long                bytesWritten             = 0L;

    private CompressionStrategy strategy;                       // algorithm.

    /**
     * The compression algorithm for lzo library.
     */
    public static enum CompressionStrategy
    {
        /**
         * lzo1 algorithms.
         */
        LZO1(0), LZO1_99(1),

        /**
         * lzo1a algorithms.
         */
        LZO1A(2), LZO1A_99(3),

        /**
         * lzo1b algorithms.
         */
        LZO1B(4), LZO1B_BEST_COMPRESSION(5), LZO1B_BEST_SPEED(6), LZO1B_1(7), LZO1B_2(
                8), LZO1B_3(9), LZO1B_4(10), LZO1B_5(11), LZO1B_6(12), LZO1B_7(
                13), LZO1B_8(14), LZO1B_9(15), LZO1B_99(16), LZO1B_999(17),

        /**
         * lzo1c algorithms.
         */
        LZO1C(18), LZO1C_BEST_COMPRESSION(19), LZO1C_BEST_SPEED(20), LZO1C_1(21), LZO1C_2(
                22), LZO1C_3(23), LZO1C_4(24), LZO1C_5(25), LZO1C_6(26), LZO1C_7(
                27), LZO1C_8(28), LZO1C_9(29), LZO1C_99(30), LZO1C_999(31),

        /**
         * lzo1f algorithms.
         */
        LZO1F_1(32), LZO1F_999(33),

        /**
         * lzo1x algorithms.
         */
        LZO1X_1(34), LZO1X_11(35), LZO1X_12(36), LZO1X_15(37), LZO1X_999(38),

        /**
         * lzo1y algorithms.
         */
        LZO1Y_1(39), LZO1Y_999(40),

        /**
         * lzo1z algorithms.
         */
        LZO1Z_999(41),

        /**
         * lzo2a algorithms.
         */
        LZO2A_999(42);

        private final int compressor;

        private CompressionStrategy(int compressor)
        {
            this.compressor = compressor;
        }

        int getCompressor()
        {
            return compressor;
        }
    }; // CompressionStrategy

    private static boolean  nativeLzoLoaded;
    public static final int LZO_LIBRARY_VERSION;

    static
    {
        if (LzoLibrary.isNativeCodeLoaded())
        {
            // Initialize the native library
            nativeLzoLoaded = true;
            LZO_LIBRARY_VERSION = (nativeLzoLoaded) ? 0xFFFF & LzoJna
                    .getLzoLibraryVersion() : -1;
        }
        else
        {
            System.out.println("Cannot load "
                    + LzoCompressor.class.getName()
                    + " without lzo2 library!");
            nativeLzoLoaded = false;
            LZO_LIBRARY_VERSION = -1;
        }
    }

    public static boolean isNativeLzoLoaded()
    {
        return nativeLzoLoaded;
    }

    /**
     * Creates a new compressor using the specified {@link CompressionStrategy}.
     * 
     * @param strategy
     *            lzo compression algorithm to use
     * @param directBufferSize
     *            size of the direct buffer to be used.
     */
    public LzoCompressor(CompressionStrategy strategy, int directBufferSize)
    {
        this.strategy = strategy;
        this.directBufferSize = directBufferSize;

        this.uncompressedDirectBuf = ByteBuffer
                .allocateDirect(directBufferSize);
        this.compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        this.compressedDirectBuf.position(directBufferSize);

        LzoJna.init();
        CompressorInfo comp = LzoJna.LzoCompressor_Jna.lzo_compressors[this.strategy
                .getCompressor()];
        this.workingMemoryBufLen = comp.getWorkingMemory();
    }

    /**
     * Creates a new compressor with the default lzo1x_1 compression.
     */
    public LzoCompressor()
    {
        this(CompressionStrategy.LZO1X_1, 64 * 1024);
    }

    public synchronized void setInput(byte[] b, int off, int len)
    {
        if (b == null)
        {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len)
        {
            throw new ArrayIndexOutOfBoundsException();
        }
        finished = false;

        if (len > uncompressedDirectBuf.remaining())
        {
            // save data; now !needsInput
            this.userBuf = b;
            this.userBufOff = off;
            this.userBufLen = len;
        }
        else
        {
            ((ByteBuffer) uncompressedDirectBuf).put(b, off, len);
            uncompressedDirectBufLen = uncompressedDirectBuf.position();
        }
        bytesRead += len;
    }

    /**
     * If a write would exceed the capacity of the direct buffers, it is set
     * aside to be loaded by this function while the compressed data are
     * consumed.
     */
    synchronized void setInputFromSavedData()
    {
        if (0 >= userBufLen)
        {
            return;
        }
        finished = false;

        uncompressedDirectBufLen = Math.min(userBufLen, directBufferSize);
        ((ByteBuffer) uncompressedDirectBuf).put(userBuf, userBufOff,
                uncompressedDirectBufLen);

        // Note how much data is being fed to lzo
        userBufOff += uncompressedDirectBufLen;
        userBufLen -= uncompressedDirectBufLen;
    }

    public synchronized void setDictionary(byte[] b, int off, int len)
    {
        // nop
    }

    /** {@inheritDoc} */
    public boolean needsInput()
    {
        return !(compressedDirectBuf.remaining() > 0
                || uncompressedDirectBuf.remaining() == 0 || userBufLen > 0);
    }

    public synchronized void finish()
    {
        finish = true;
    }

    public synchronized boolean finished()
    {
        // Check if 'lzo' says its 'finished' and
        // all compressed data has been consumed
        return (finish && finished && compressedDirectBuf.remaining() == 0);
    }

    public synchronized int compress(byte[] b, int off, int len)
            throws IOException
    {
        if (b == null)
        {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len)
        {
            throw new ArrayIndexOutOfBoundsException();
        }

        // Check if there is compressed data
        int n = compressedDirectBuf.remaining();
        if (n > 0)
        {
            n = Math.min(n, len);
            ((ByteBuffer) compressedDirectBuf).get(b, off, n);
            bytesWritten += n;
            return n;
        }

        // Re-initialize the lzo's output direct-buffer
        compressedDirectBuf.clear();
        compressedDirectBuf.limit(0);
        if (0 == uncompressedDirectBuf.position())
        {
            // No compressed data, so we should have !needsInput or !finished
            setInputFromSavedData();
            if (0 == uncompressedDirectBuf.position())
            {
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
        if (0 == userBufLen)
        {
            finished = true;
        }

        // Get atmost 'len' bytes
        n = Math.min(n, len);
        bytesWritten += n;
        ((ByteBuffer) compressedDirectBuf).get(b, off, n);

        return n;
    }

    public synchronized void reset()
    {
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
    public synchronized long getBytesRead()
    {
        return bytesRead;
    }

    /**
     * Return number of bytes consumed by callers of compress since last reset.
     */
    public synchronized long getBytesWritten()
    {
        return bytesWritten;
    }

    /**
     * Return the uncompressed byte buffer contents, for use when the compressed
     * block would be larger than the uncompressed block, because the LZO spec
     * dictates that the uncompressed bytes are written to the file in this
     * case.
     */
    public byte[] uncompressedBytes()
    {
        byte[] b = new byte[(int) bytesRead];
        ((ByteBuffer) uncompressedDirectBuf).get(b);
        return b;
    }

    /**
     * Noop.
     */
    public synchronized void end()
    {
        // nop
    }

    public CompressionStrategy getStrategy()
    {
        return this.strategy;
    }

    protected int compressBytesDirect(int compressor)
    {
        int rv = -1;
        CompressorInfo info = LzoJna.LzoCompressor_Jna.lzo_compressors[compressor];
        Function func = LzoJna.getFunction(info.getFunction());
        NativeLong src_len = new NativeLong(this.uncompressedDirectBufLen);
        NativeLongByReference dst_len = new NativeLongByReference();
        Pointer wrkmem = new Memory(this.workingMemoryBufLen);
        int compressionLevel = info.getCompressionLevel();
        if (compressionLevel == LzoJna.UNDEFINED_COMPRESSION_LEVEL)
        {
            rv = func.invokeInt(new Object[]
            { uncompressedDirectBuf, src_len, compressedDirectBuf, dst_len,
                    wrkmem });
        }
        else
        {
            rv = func.invokeInt(new Object[]
            { uncompressedDirectBuf, src_len, compressedDirectBuf, dst_len,
                    wrkmem, compressionLevel });
        }
        if (rv == LzoLibrary.LZO_E_OK)
        {
            this.uncompressedDirectBufLen = 0;
        }
        else
        {
            throw new InternalError(func.getName() + " returned: " + rv);
        }
        return dst_len.getValue().intValue();
    }
}
