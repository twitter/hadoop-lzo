package com.hadoop.compression.lzo.jna;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.compress.Decompressor;

import com.sun.jna.Function;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.NativeLongByReference;

public class LzoDecompressor implements Decompressor
{
    private int                 directBufferSize;
    private byte[]              userBuf                    = null;
    private int                 userBufOff                 = 0;
    private int                 userBufLen                 = 0;
    private Buffer              compressedDirectBuf        = null;
    private int                 compressedDirectBufLen     = 0;
    private Buffer              uncompressedDirectBuf      = null;
    private boolean             finished                   = false;
    private boolean             isCurrentBlockUncompressed = false;
    /**
     * The minimum version of LZO that we can read. Set to 1.0 since there were
     * a couple header size changes prior to that. See read_header() in lzop.c
     */
    public static int           MINIMUM_LZO_VERSION        = 0x0100;

    private CompressionStrategy strategy;

    public static enum CompressionStrategy
    {
        /**
         * lzo1 algorithms.
         */
        LZO1(0),

        /**
         * lzo1a algorithms.
         */
        LZO1A(1),

        /**
         * lzo1b algorithms.
         */
        LZO1B(2), LZO1B_SAFE(3),

        /**
         * lzo1c algorithms.
         */
        LZO1C(4), LZO1C_SAFE(5), LZO1C_ASM(6), LZO1C_ASM_SAFE(7),

        /**
         * lzo1f algorithms.
         */
        LZO1F(8), LZO1F_SAFE(9), LZO1F_ASM_FAST(10), LZO1F_ASM_FAST_SAFE(11),

        /**
         * lzo1x algorithms.
         */
        LZO1X(12), LZO1X_SAFE(13), LZO1X_ASM(14), LZO1X_ASM_SAFE(15), LZO1X_ASM_FAST(
                16), LZO1X_ASM_FAST_SAFE(17),

        /**
         * lzo1y algorithms.
         */
        LZO1Y(18), LZO1Y_SAFE(19), LZO1Y_ASM(20), LZO1Y_ASM_SAFE(21), LZO1Y_ASM_FAST(
                22), LZO1Y_ASM_FAST_SAFE(23),

        /**
         * lzo1z algorithms.
         */
        LZO1Z(24), LZO1Z_SAFE(25),

        /**
         * lzo2a algorithms.
         */
        LZO2A(26), LZO2A_SAFE(27);

        private final int decompressor;

        private CompressionStrategy(int decompressor)
        {
            this.decompressor = decompressor;
        }

        int getDecompressor()
        {
            return decompressor;
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
                    + LzoDecompressor.class.getName()
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
     * Creates a new lzo decompressor.
     * 
     * @param strategy
     *            lzo decompression algorithm
     */
    public LzoDecompressor(CompressionStrategy strategy, int directBufferSize)
    {
        this.strategy = strategy;
        this.directBufferSize = directBufferSize;

        this.compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        this.uncompressedDirectBuf = ByteBuffer
                .allocateDirect(directBufferSize);
        this.uncompressedDirectBuf.position(directBufferSize);

        LzoJna.init();
    }

    /**
     * Creates a new lzo decompressor.
     */
    public LzoDecompressor()
    {
        this(CompressionStrategy.LZO1X, 64 * 1024);
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

        this.userBuf = b;
        this.userBufOff = off;
        this.userBufLen = len;

        setInputFromSavedData();

        // Reinitialize lzo's output direct-buffer
        uncompressedDirectBuf.limit(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);
    }

    synchronized void setInputFromSavedData()
    {
        // If the current block is stored uncompressed, no need
        // to ready all the lzo machinery, because it will be bypassed.
        if (!isCurrentBlockUncompressed())
        {
            compressedDirectBufLen = Math.min(userBufLen, directBufferSize);

            // Reinitialize lzo's input direct-buffer
            compressedDirectBuf.rewind();
            ((ByteBuffer) compressedDirectBuf).put(userBuf, userBufOff,
                    compressedDirectBufLen);

            // Note how much data is being fed to lzo
            userBufOff += compressedDirectBufLen;
            userBufLen -= compressedDirectBufLen;
        }
    }

    public synchronized void setDictionary(byte[] b, int off, int len)
    {
        // nop
    }

    public synchronized boolean needsInput()
    {
        // Consume remaining compressed data?
        if (uncompressedDirectBuf.remaining() > 0)
        {
            return false;
        }

        // Check if lzo has consumed all input
        if (compressedDirectBufLen <= 0)
        {
            // Check if we have consumed all user-input
            if (userBufLen <= 0)
            {
                return true;
            }
            else
            {
                setInputFromSavedData();
            }
        }

        return false;
    }

    public synchronized boolean needsDictionary()
    {
        return false;
    }

    public synchronized boolean finished()
    {
        // Check if 'lzo' says its 'finished' and
        // all uncompressed data has been consumed
        return (finished && uncompressedDirectBuf.remaining() == 0);
    }

    public synchronized int decompress(byte[] b, int off, int len)
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

        int numBytes = 0;
        if (isCurrentBlockUncompressed())
        {
            // The current block has been stored uncompressed, so just
            // copy directly from the input buffer.
            numBytes = Math.min(userBufLen, len);
            System.arraycopy(userBuf, userBufOff, b, off, numBytes);
            userBufOff += numBytes;
            userBufLen -= numBytes;
        }
        else
        {
            // Check if there is uncompressed data
            numBytes = uncompressedDirectBuf.remaining();
            if (numBytes > 0)
            {
                numBytes = Math.min(numBytes, len);
                ((ByteBuffer) uncompressedDirectBuf).get(b, off, numBytes);
                return numBytes;
            }

            // Check if there is data to decompress
            if (compressedDirectBufLen <= 0)
            {
                return 0;
            }

            // Re-initialize the lzo's output direct-buffer
            uncompressedDirectBuf.rewind();
            uncompressedDirectBuf.limit(directBufferSize);

            // Decompress data
            numBytes = decompressBytesDirect(strategy.getDecompressor());
            uncompressedDirectBuf.limit(numBytes);
            compressedDirectBuf.clear();

            // Return atmost 'len' bytes
            numBytes = Math.min(numBytes, len);
            ((ByteBuffer) uncompressedDirectBuf).get(b, off, numBytes);
        }

        // Set 'finished' if lzo has consumed all user-data
        if (userBufLen <= 0)
        {
            finished = true;
        }

        return numBytes;
    }

    public int getRemaining()
    {
        return compressedDirectBuf.remaining();
    }

    public synchronized void reset()
    {
        finished = false;
        compressedDirectBufLen = 0;
        uncompressedDirectBuf.limit(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);
        userBufOff = userBufLen = 0;
    }

    public synchronized void end()
    {
        // nop
    }

    @Override
    protected void finalize()
    {
        end();
    }

    /**
     * Note whether the current block being decompressed is actually stored as
     * uncompressed data. If it is, there is no need to use the lzo
     * decompressor, and no need to update compressed checksums.
     * 
     * @param uncompressed
     *            Whether the current block of data is uncompressed already.
     */
    public synchronized void setCurrentBlockUncompressed(boolean uncompressed)
    {
        isCurrentBlockUncompressed = uncompressed;
    }

    /**
     * Query the compression status of the current block as it exists in the
     * file.
     * 
     * @return true if the current block of data was stored as uncompressed.
     */
    protected synchronized boolean isCurrentBlockUncompressed()
    {
        return isCurrentBlockUncompressed;
    }

    protected int decompressBytesDirect(int decompressor)
    {
        String function = LzoJna.LzoDecompressor_Jna.lzo_decompressors[decompressor];
        Function func = LzoJna.getFunction(function);
        NativeLong src_len = new NativeLong(this.compressedDirectBufLen);
        NativeLongByReference dst_len = new NativeLongByReference();
        int rv = func.invokeInt(new Object[]
        { compressedDirectBuf, src_len, uncompressedDirectBuf, dst_len,
                Pointer.NULL });
        if (rv == LzoLibrary.LZO_E_OK)
        {
            this.compressedDirectBufLen = 0;
        }
        else
        {
            throw new InternalError(func.getName() + " returned: " + rv);
        }
        return dst_len.getValue().intValue();
    }
}
