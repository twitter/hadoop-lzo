package com.hadoop.compression.lzo.jna;

import com.hadoop.compression.lzo.jna.LzoLibrary.lzo_callback_t;
import com.sun.jna.Function;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

public class LzoJna
{
    public static final int UNDEFINED_COMPRESSION_LEVEL = -999;

    public static void init()
    {
        int rv = LzoLibrary.__lzo_init_v2(LzoLibrary.LZO_VERSION,
                LzoLibrary.LZO_SIZEOF_SHORT, LzoLibrary.LZO_SIZEOF_INT,
                LzoLibrary.LZO_SIZEOF_LONG, NativeLong.SIZE, NativeLong.SIZE,
                LzoLibrary.LZO_SIZEOF_VOID_P, Pointer.SIZE,
                LzoLibrary.LZO_SIZEOF_VOID_P, new lzo_callback_t().size());
        if (rv != LzoLibrary.LZO_E_OK)
        {
            throw new InternalError("Could not initialize lzo library!");
        }
    }

    public static Function getFunction(String functionName)
    {
        Function function = LzoLibrary.JNA_NATIVE_LIB.getFunction(functionName);
        return function;
    }

    public static int getLzoLibraryVersion()
    {
        return LzoLibrary.lzo_version();
    }

    public static class LzoCompressor_Jna
    {
        public static final CompressorInfo[] lzo_compressors =
                                                             {
                                                                     /**
                                                                      * lzo1
                                                                      * compressors
                                                                      */
                                                                     /* 0 */new CompressorInfo(
                                                                             "lzo1_compress",
                                                                             8192 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 1 */new CompressorInfo(
                                                                             "lzo1_99_compress",
                                                                             65536 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /**
                                                                      * lzo1a
                                                                      * compressors
                                                                      */
                                                                     /* 2 */new CompressorInfo(
                                                                             "lzo1a_compress",
                                                                             8192 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 3 */new CompressorInfo(
                                                                             "lzo1a_99_compress",
                                                                             65536 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /**
                                                                      * lzo1b
                                                                      * compressors
                                                                      */
                                                                     /* 4 */new CompressorInfo(
                                                                             "lzo1b_compress",
                                                                             16384 * 4,
                                                                             LzoLibrary.LZO1B_DEFAULT_COMPRESSION),
                                                                     /* 5 */new CompressorInfo(
                                                                             "lzo1b_compress",
                                                                             16384 * 4,
                                                                             LzoLibrary.LZO1B_BEST_SPEED),
                                                                     /* 6 */new CompressorInfo(
                                                                             "lzo1b_compress",
                                                                             16384 * 4,
                                                                             LzoLibrary.LZO1B_BEST_COMPRESSION),
                                                                     /* 7 */new CompressorInfo(
                                                                             "lzo1b_1_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 8 */new CompressorInfo(
                                                                             "lzo1b_2_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 9 */new CompressorInfo(
                                                                             "lzo1b_3_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 10 */new CompressorInfo(
                                                                             "lzo1b_4_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 11 */new CompressorInfo(
                                                                             "lzo1b_5_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 12 */new CompressorInfo(
                                                                             "lzo1b_6_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 13 */new CompressorInfo(
                                                                             "lzo1b_7_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 14 */new CompressorInfo(
                                                                             "lzo1b_8_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 15 */new CompressorInfo(
                                                                             "lzo1b_9_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 16 */new CompressorInfo(
                                                                             "lzo1b_99_compress",
                                                                             65536 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 17 */new CompressorInfo(
                                                                             "lzo1b_999_compress",
                                                                             3 * 65536 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /**
                                                                      * lzo1c
                                                                      * compressors
                                                                      */
                                                                     /* 18 */new CompressorInfo(
                                                                             "lzo1c_compress",
                                                                             16384 * 4,
                                                                             LzoLibrary.LZO1C_DEFAULT_COMPRESSION),
                                                                     /* 19 */new CompressorInfo(
                                                                             "lzo1c_compress",
                                                                             16384 * 4,
                                                                             LzoLibrary.LZO1C_BEST_SPEED),
                                                                     /* 20 */new CompressorInfo(
                                                                             "lzo1c_compress",
                                                                             16384 * 4,
                                                                             LzoLibrary.LZO1C_BEST_COMPRESSION),
                                                                     /* 21 */new CompressorInfo(
                                                                             "lzo1c_1_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 22 */new CompressorInfo(
                                                                             "lzo1c_2_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 23 */new CompressorInfo(
                                                                             "lzo1c_3_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 24 */new CompressorInfo(
                                                                             "lzo1c_4_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 25 */new CompressorInfo(
                                                                             "lzo1c_5_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 26 */new CompressorInfo(
                                                                             "lzo1c_6_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 27 */new CompressorInfo(
                                                                             "lzo1c_7_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 28 */new CompressorInfo(
                                                                             "lzo1c_8_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 29 */new CompressorInfo(
                                                                             "lzo1c_9_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 30 */new CompressorInfo(
                                                                             "lzo1c_99_compress",
                                                                             65536 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 31 */new CompressorInfo(
                                                                             "lzo1c_999_compress",
                                                                             5 * 16384 * LzoLibrary.LZO_SIZEOF_SHORT,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /**
                                                                      * lzo1f
                                                                      * compressors
                                                                      */
                                                                     /* 32 */new CompressorInfo(
                                                                             "lzo1f_1_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 33 */new CompressorInfo(
                                                                             "lzo1f_999_compress",
                                                                             5 * 16384 * LzoLibrary.LZO_SIZEOF_SHORT,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /**
                                                                      * lzo1x
                                                                      * compressors
                                                                      */
                                                                     /* 34 */new CompressorInfo(
                                                                             "lzo1x_1_compress",
                                                                             16384 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 35 */new CompressorInfo(
                                                                             "lzo1x_11_compress",
                                                                             2048 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 36 */new CompressorInfo(
                                                                             "lzo1x_12_compress",
                                                                             4096 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 37 */new CompressorInfo(
                                                                             "lzo1x_15_compress",
                                                                             32768 * 4,
                                                                             UNDEFINED_COMPRESSION_LEVEL),
                                                                     /* 38 */new CompressorInfo(
                                                                             "lzo1x_999_compress",
                                                                             14 * 16384 * LzoLibrary.LZO_SIZEOF_SHORT,
                                                                             UNDEFINED_COMPRESSION_LEVEL)
                                                             //
                                                             // /**
                                                             // lzo1y
                                                             // compressors
                                                             // */
                                                             // /*
                                                             // 39
                                                             // */new
                                                             // CompressorInfo("lzo1y_1_compress",
                                                             // LZO1Y_MEM_COMPRESS,
                                                             // UNDEFINED_COMPRESSION_LEVEL),
                                                             // /*
                                                             // 40
                                                             // */new
                                                             // CompressorInfo("lzo1y_999_compress",
                                                             // LZO1Y_999_MEM_COMPRESS,
                                                             // UNDEFINED_COMPRESSION_LEVEL),
                                                             //
                                                             // /**
                                                             // lzo1z
                                                             // compressors
                                                             // */
                                                             // /*
                                                             // 41
                                                             // */new
                                                             // CompressorInfo("lzo1z_999_compress",
                                                             // LZO1Z_999_MEM_COMPRESS,
                                                             // UNDEFINED_COMPRESSION_LEVEL),
                                                             //
                                                             // /**
                                                             // lzo2a
                                                             // compressors
                                                             // */
                                                             // /*
                                                             // 42
                                                             // */new
                                                             // CompressorInfo("lzo2a_999_compress",
                                                             // LZO2A_999_MEM_COMPRESS,
                                                             // UNDEFINED_COMPRESSION_LEVEL)
                                                             };
    }

    public static class LzoDecompressor_Jna
    {
        // The lzo 'decompressors'
        public static final String[] lzo_decompressors =
                                                       {
                                                               /**
                                                                * lzo1
                                                                * decompressors
                                                                */
                                                               /* 0 */"lzo1_decompress",

                                                               /**
                                                                * lzo1a
                                                                * compressors
                                                                */
                                                               /* 1 */"lzo1a_decompress",

                                                               /**
                                                                * lzo1b
                                                                * compressors
                                                                */
                                                               /* 2 */"lzo1b_decompress",
                                                               /* 3 */"lzo1b_decompress_safe",

                                                               /**
                                                                * lzo1c
                                                                * compressors
                                                                */
                                                               /* 4 */"lzo1c_decompress",
                                                               /* 5 */"lzo1c_decompress_safe",
                                                               /* 6 */"lzo1c_decompress_asm",
                                                               /* 7 */"lzo1c_decompress_asm_safe",

                                                               /**
                                                                * lzo1f
                                                                * compressors
                                                                */
                                                               /* 8 */"lzo1f_decompress",
                                                               /* 9 */"lzo1f_decompress_safe",
                                                               /* 10 */"lzo1f_decompress_asm_fast",
                                                               /* 11 */"lzo1f_decompress_asm_fast_safe",

                                                               /**
                                                                * lzo1x
                                                                * compressors
                                                                */
                                                               /* 12 */"lzo1x_decompress",
                                                               /* 13 */"lzo1x_decompress_safe",
                                                               /* 14 */"lzo1x_decompress_asm",
                                                               /* 15 */"lzo1x_decompress_asm_safe",
                                                               /* 16 */"lzo1x_decompress_asm_fast",
                                                               /* 17 */"lzo1x_decompress_asm_fast_safe",

                                                               /**
                                                                * lzo1y
                                                                * compressors
                                                                */
                                                               /* 18 */"lzo1y_decompress",
                                                               /* 19 */"lzo1y_decompress_safe",
                                                               /* 20 */"lzo1y_decompress_asm",
                                                               /* 21 */"lzo1y_decompress_asm_safe",
                                                               /* 22 */"lzo1y_decompress_asm_fast",
                                                               /* 23 */"lzo1y_decompress_asm_fast_safe",

                                                               /**
                                                                * lzo1z
                                                                * compressors
                                                                */
                                                               /* 24 */"lzo1z_decompress",
                                                               /* 25 */"lzo1z_decompress_safe",

                                                               /**
                                                                * lzo2a
                                                                * compressors
                                                                */
                                                               /* 26 */"lzo2a_decompress",
                                                               /* 27 */"lzo2a_decompress_safe" };
    }

    public static class CompressorInfo
    {
        private String function;
        private int    workingMemory;
        private int    compressionLevel;

        public CompressorInfo(String function, int workingMemory,
                int compressionLevel)
        {
            this.function = function;
            this.workingMemory = workingMemory;
            this.compressionLevel = compressionLevel;
        }

        public int getCompressionLevel()
        {
            return compressionLevel;
        }

        public String getFunction()
        {
            return function;
        }

        public int getWorkingMemory()
        {
            return workingMemory;
        }
    }
}
