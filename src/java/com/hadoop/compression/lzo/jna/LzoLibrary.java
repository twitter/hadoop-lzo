package com.hadoop.compression.lzo.jna;

import java.nio.ByteBuffer;

import com.sun.jna.Callback;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.Union;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.NativeLongByReference;

public class LzoLibrary implements Library
{
    public static class __lzo_pu_u extends Union
    {
        public static class ByReference extends __lzo_pu_u implements
                Structure.ByReference
        {

        }

        public static class ByValue extends __lzo_pu_u implements
                Structure.ByValue
        {

        }

        public Pointer    p;

        public NativeLong u;

        public __lzo_pu_u()
        {
            super();
        }

        public __lzo_pu_u(NativeLong u)
        {
            super();
            this.u = u;
            setType(NativeLong.class);
        };

        public __lzo_pu_u(Pointer p)
        {
            super();
            this.p = p;
            setType(Pointer.class);
        };
    }

    public static class __lzo_pu32_u extends Union
    {
        public static class ByReference extends __lzo_pu32_u implements
                Structure.ByReference
        {

        }

        public static class ByValue extends __lzo_pu32_u implements
                Structure.ByValue
        {

        }

        public Pointer p;

        public int     u32;

        public __lzo_pu32_u()
        {
            super();
        }

        public __lzo_pu32_u(int u32)
        {
            super();
            this.u32 = u32;
            setType(Integer.TYPE);
        };

        public __lzo_pu32_u(Pointer p)
        {
            super();
            this.p = p;
            setType(Pointer.class);
        };
    }

    public static class lzo_align_t extends Union
    {
        public static class ByReference extends lzo_align_t implements
                Structure.ByReference
        {

        }

        public static class ByValue extends lzo_align_t implements
                Structure.ByValue
        {

        }

        public Pointer    vp;
        public Pointer    bp;

        public int        u32;

        public NativeLong l;

        public lzo_align_t()
        {
            super();
        }

        public lzo_align_t(int u32)
        {
            super();
            this.u32 = u32;
            setType(Integer.TYPE);
        }

        public lzo_align_t(NativeLong l)
        {
            super();
            this.l = l;
            setType(NativeLong.class);
        };

        public lzo_align_t(Pointer vp_or_bp)
        {
            super();
            this.bp = this.vp = vp_or_bp;
            setType(Pointer.class);
        };
    }

    public interface lzo_alloc_func_t extends Callback
    {
        Pointer apply(lzo_callback_t self, NativeLong items, NativeLong size);
    }

    public static class lzo_callback_t extends Structure
    {
        public static class ByReference extends lzo_callback_t implements
                Structure.ByReference
        {

        }

        public static class ByValue extends lzo_callback_t implements
                Structure.ByValue
        {

        }

        /**
         * custom allocators (set to 0 to disable)<br>
         * [not used right now]
         */
        public lzo_alloc_func_t    nalloc;
        // / [not used right now]
        public lzo_free_func_t     nfree;
        // / a progress indicator callback function (set to 0 to disable)
        public lzo_progress_func_t nprogress;
        /**
         * NOTE: the first parameter "self" of the nalloc/nfree/nprogress<br>
         * callbacks points back to this struct, so you are free to store<br>
         * some extra info in the following variables.
         */
        public Pointer             user1;

        public NativeLong          user2;

        public NativeLong          user3;

        public lzo_callback_t()
        {
            super();
        };

        public lzo_callback_t(lzo_alloc_func_t nalloc, lzo_free_func_t nfree,
                lzo_progress_func_t nprogress, Pointer user1, NativeLong user2,
                NativeLong user3)
        {
            super();
            this.nalloc = nalloc;
            this.nfree = nfree;
            this.nprogress = nprogress;
            this.user1 = user1;
            this.user2 = user2;
            this.user3 = user3;
        };
    }

    public interface lzo_compress_dict_t extends Callback
    {
        int apply(Pointer src, NativeLong src_len, Pointer dst,
                NativeLongByReference dst_len, Pointer wrkmem, Pointer dict,
                NativeLong dict_len);
    }

    public interface lzo_compress_t extends Callback
    {
        int apply(Pointer src, NativeLong src_len, Pointer dst,
                NativeLongByReference dst_len, Pointer wrkmem);
    }

    public interface lzo_compress2_t extends Callback
    {
        int apply(Pointer src, NativeLong src_len, Pointer dst,
                NativeLongByReference dst_len, Pointer wrkmem,
                int compression_level);
    }

    public interface lzo_decompress_dict_t extends Callback
    {
        int apply(Pointer src, NativeLong src_len, Pointer dst,
                NativeLongByReference dst_len, Pointer wrkmem, Pointer dict,
                NativeLong dict_len);
    }

    public interface lzo_decompress_t extends Callback
    {
        int apply(Pointer src, NativeLong src_len, Pointer dst,
                NativeLongByReference dst_len, Pointer wrkmem);
    }

    public interface lzo_free_func_t extends Callback
    {
        void apply(lzo_callback_t self, Pointer ptr);
    }

    public interface lzo_optimize_t extends Callback
    {
        int apply(Pointer src, NativeLong src_len, Pointer dst,
                NativeLongByReference dst_len, Pointer wrkmem);
    }

    public interface lzo_progress_func_t extends Callback
    {
        void apply(lzo_callback_t lzo_callback_tPtr1, NativeLong lzo_uint1,
                NativeLong lzo_uint2, int int1);
    };

    private static boolean      nativeLibraryLoaded                 = false;
    public static final String  JNA_LIBRARY_NAME                    = "lzo2";
    public static NativeLibrary JNA_NATIVE_LIB;

    static
    {
        try
        {
            JNA_NATIVE_LIB = NativeLibrary.getInstance(JNA_LIBRARY_NAME);
            nativeLibraryLoaded = true;
            Native.register(JNA_LIBRARY_NAME);
            System.out
                    .println("Loaded native " + JNA_LIBRARY_NAME + " library");
        }
        catch (Throwable t)
        {
            System.out.println("Could not load native " + JNA_LIBRARY_NAME
                    + " library");
            nativeLibraryLoaded = false;
        }
    }

    public static final int     __lzo_HAVE_inline                   = 1;

    public static final int     NAME_MAX                            = 255;
    public static final int     _POSIX2_CHARCLASS_NAME_MAX          = 14;
    public static final int     __GLIBC_HAVE_LONG_LONG              = 1;
    public static final int     RTSIG_MAX                           = 32;
    public static final int     LZO_OPT_UNALIGNED32                 = 1;
    public static final int     SCHAR_MAX                           = 127;
    public static final int     LZO2A_MEM_DECOMPRESS                = (0);
    public static final int     _POSIX2_COLL_WEIGHTS_MAX            = 2;
    public static final int     LZO_OPT_UNALIGNED16                 = 1;
    public static final int     AIO_PRIO_DELTA_MAX                  = 20;
    public static final int     __STDC_IEC_559__                    = 1;
    public static final String  LZO_INFO_CCVER                      = "unknown";
    public static final int     SEM_VALUE_MAX                       = (2147483647);
    public static final int     __USE_XOPEN2K8                      = 1;
    public static final int     __GLIBC__                           = 2;
    public static final int     __USE_FORTIFY_LEVEL                 = 0;
    public static final int     LZO_E_OUT_OF_MEMORY                 = (-2);
    public static final int     PTHREAD_STACK_MIN                   = 16384;
    public static final int     LZO1Z_MEM_DECOMPRESS                = (0);
    public static final int     ULLONG_MAX                          = -1;
    public static final int     LZO_MM_FLAT                         = 1;
    public static final int     _POSIX_SYMLOOP_MAX                  = 8;
    public static final int     _POSIX2_EXPR_NEST_MAX               = 32;
    public static final int     __USE_POSIX199309                   = 1;
    public static final int     _POSIX_MAX_INPUT                    = 255;
    public static final int     NGROUPS_MAX                         = 65536;
    public static final int     LZO_E_INPUT_OVERRUN                 = (-4);
    public static final int     LZO_E_LOOKBEHIND_OVERRUN            = (-6);
    public static final int     MAX_INPUT                           = 255;
    public static final int     LZO_E_OK                            = 0;
    public static final int     __USE_BSD                           = 1;
    public static final int     __GNU_LIBRARY__                     = 6;
    public static final int     LZO1B_BEST_SPEED                    = 1;
    public static final long    UINT_MAX                            = 4294967295L;
    public static final String  LZO_VERSION_STRING                  = "2.03";
    public static final String  LZO_INFO_OS                         = "win32";
    public static final int     _SVID_SOURCE                        = 1;
    public static final int     SCHAR_MIN                           = (-128);
    public static final int     _POSIX_RTSIG_MAX                    = 8;
    public static final int     LZO_ARCH_IA32                       = 1;
    public static final int     LZO_E_EOF_NOT_FOUND                 = (-7);
    public static final String  __LZO_INFOSTR_CCVER                 = " ";
    public static final int     _BITS_POSIX1_LIM_H                  = 1;
    public static final int     _POSIX_LINK_MAX                     = 8;
    public static final int     DELAYTIMER_MAX                      = 2147483647;
    public static final long    ULONG_MAX                           = 4294967295L;
    public static final int     LZO_0xffffL                         = 65535;
    public static final int     _POSIX_SIGQUEUE_MAX                 = 32;
    public static final int     _BSD_SOURCE                         = 1;
    public static final String  LZO_INFO_ABI_PM                     = "ilp32";
    public static final int     USHRT_MAX                           = 65535;
    public static final int     __USE_POSIX                         = 1;
    public static final String  LZO_INFO_MM                         = "flat";
    public static final long    LLONG_MAX                           = 9223372036854775807L;
    public static final int     __USE_SVID                          = 1;
    public static final int     TTY_NAME_MAX                        = 32;
    public static final int     __USE_ISOC99                        = 1;
    public static final int     LZO_SIZEOF_SHORT                    = 2;
    public static final int     LZO1C_MEM_DECOMPRESS                = (0);
    public static final int     LZO_CC_UNKNOWN                      = 1;
    public static final int     _POSIX_TTY_NAME_MAX                 = 9;
    public static final int     _POSIX_MQ_OPEN_MAX                  = 8;
    public static final int     LZO1_MEM_DECOMPRESS                 = (0);
    public static final int     LZO_E_ERROR                         = (-1);
    public static final int     _POSIX_THREAD_DESTRUCTOR_ITERATIONS = 4;
    public static final String  __LZO_INFOSTR_LIBC                  = ".";
    public static final int     _POSIX_SEM_NSEMS_MAX                = 256;
    public static final int     _POSIX_TZNAME_MAX                   = 6;
    public static final int     __STDC_ISO_10646__                  = 200009;
    public static final int     __LZODEFS_H_INCLUDED                = 1;
    public static final int     LZO_ARCH_I386                       = 1;
    public static final int     _POSIX_STREAM_MAX                   = 8;
    public static final int     _POSIX_NAME_MAX                     = 14;
    public static final int     LONG_MAX                            = 2147483647;
    public static final int     LZO_VERSION                         = 8240;
    public static final int     _POSIX_THREAD_KEYS_MAX              = 128;
    public static final int     LZO1Z_MEM_OPTIMIZE                  = (0);
    public static final String  __LZO_INFOSTR_PM                    = ".";
    public static final int     PATH_MAX                            = 4096;
    public static final int     _POSIX2_LINE_MAX                    = 2048;
    public static final int     LZO_ABI_LITTLE_ENDIAN               = 1;
    public static final int     LZO1C_BEST_SPEED                    = 1;
    public static final int     _POSIX_AIO_LISTIO_MAX               = 2;
    public static final String  LZO_INFO_ABI_ENDIAN                 = "le";
    public static final int     LZO_SIZEOF_LONG                     = 4;
    public static final int     _POSIX_C_SOURCE                     = 200809;
    public static final long    LZO_0xffffffffL                     = 4294967295L;
    public static final int     XATTR_SIZE_MAX                      = 65536;
    public static final int     LZO1C_BEST_COMPRESSION              = 9;
    public static final int     _POSIX2_RE_DUP_MAX                  = 255;
    public static final int     __USE_XOPEN2K                       = 1;
    public static final int     _POSIX_CLOCKRES_MIN                 = 20000000;
    public static final int     _POSIX2_BC_SCALE_MAX                = 99;
    public static final int     __GLIBC_MINOR__                     = 11;
    public static final int     __STDC_CONSTANT_MACROS              = 1;
    public static final int     _POSIX_CHILD_MAX                    = 25;
    public static final String  LZO_INFO_LIBC                       = "glibc";
    public static final int     COLL_WEIGHTS_MAX                    = 255;
    public static final int     XATTR_NAME_MAX                      = 255;
    public static final int     LZO_HAVE_WINDOWS_H                  = 1;
    public static final int     _POSIX_MAX_CANON                    = 255;
    public static final int     SHRT_MIN                            = (-32768);
    public static final int     LZO1B_BEST_COMPRESSION              = 9;
    public static final int     PIPE_BUF                            = 4096;
    public static final int     _FEATURES_H                         = 1;
    public static final int     __WORDSIZE                          = 32;
    public static final int     _POSIX_PATH_MAX                     = 256;
    public static final int     LZO_E_NOT_COMPRESSIBLE              = (-3);
    public static final int     __USE_POSIX2                        = 1;
    public static final int     _POSIX_SYMLINK_MAX                  = 255;
    public static final int     __STDC_LIMIT_MACROS                 = 1;
    public static final int     CHAR_BIT                            = 8;
    public static final int     MQ_PRIO_MAX                         = 32768;
    public static final int     __USE_ATFILE                        = 1;
    public static final int     LZO1X_MEM_OPTIMIZE                  = (0);
    public static final int     MB_LEN_MAX                          = 16;
    public static final int     _POSIX_DELAYTIMER_MAX               = 32;
    public static final int     _POSIX2_BC_STRING_MAX               = 1000;
    public static final int     LZO1Y_MEM_OPTIMIZE                  = (0);
    public static final int     LZO1C_DEFAULT_COMPRESSION           = (-1);
    public static final int     _POSIX_HOST_NAME_MAX                = 255;
    public static final int     __USE_POSIX199506                   = 1;
    public static final String  __LZO_INFOSTR_ENDIAN                = ".";
    public static final int     __USE_POSIX_IMPLICITLY              = 1;
    public static final int     LZO_SIZEOF_INT                      = 4;
    public static final int     _POSIX_TIMER_MAX                    = 32;
    public static final int     _POSIX_SEM_VALUE_MAX                = 32767;
    public static final int     __USE_MISC                          = 1;
    public static final int     HOST_NAME_MAX                       = 64;
    public static final int     __USE_ANSI                          = 1;
    public static final int     _POSIX_NGROUPS_MAX                  = 8;
    public static final int     LZO_OS_WIN32                        = 1;
    public static final int     _LIBC_LIMITS_H_                     = 1;
    public static final int     LZO1F_MEM_DECOMPRESS                = (0);
    public static final int     _POSIX_PIPE_BUF                     = 512;
    public static final int     CHARCLASS_NAME_MAX                  = 2048;
    public static final int     LZO_E_INPUT_NOT_CONSUMED            = (-8);
    public static final int     _POSIX_OPEN_MAX                     = 20;
    public static final int     RE_DUP_MAX                          = (2047);
    public static final int     LZO1B_DEFAULT_COMPRESSION           = (-1);
    public static final int     LZO1X_MEM_DECOMPRESS                = (0);
    public static final int     XATTR_LIST_MAX                      = 65536;
    public static final int     LZO1B_MEM_DECOMPRESS                = (0);
    public static final int     SHRT_MAX                            = 32767;
    public static final int     _BITS_POSIX2_LIM_H                  = 1;
    public static final int     LOGIN_NAME_MAX                      = 256;
    public static final int     LZO1A_MEM_DECOMPRESS                = (0);
    public static final int     _POSIX_LOGIN_NAME_MAX               = 9;
    public static final int     _POSIX2_BC_BASE_MAX                 = 99;
    public static final String  LZO_VERSION_DATE                    = "Apr 30 2008";
    public static final int     LZO1Y_MEM_DECOMPRESS                = (0);
    public static final int     _POSIX_SOURCE                       = 1;
    public static final int     _POSIX_RE_DUP_MAX                   = 255;
    public static final String  LZO_INFO_CC                         = "unknown";
    public static final int     PTHREAD_KEYS_MAX                    = 1024;
    public static final int     _LIMITS_H                           = 1;
    public static final int     _POSIX_ARG_MAX                      = 4096;
    public static final int     _SYS_CDEFS_H                        = 1;
    public static final int     __STDC_IEC_559_COMPLEX__            = 1;
    public static final int     LZO_E_NOT_YET_IMPLEMENTED           = (-9);
    public static final int     UCHAR_MAX                           = 255;
    public static final int     INT_MAX                             = 2147483647;
    public static final String  LZO_INFO_ARCH                       = "i386";
    public static final int     LZO_ABI_ILP32                       = 1;
    public static final int     LZO_E_OUTPUT_OVERRUN                = (-5);
    public static final int     MAX_CANON                           = 255;
    public static final int     _POSIX_MQ_PRIO_MAX                  = 32;
    public static final int     _ATFILE_SOURCE                      = 1;
    public static final int     _POSIX_THREAD_THREADS_MAX           = 64;
    public static final int     _POSIX_AIO_MAX                      = 1;
    public static final int     _POSIX_SSIZE_MAX                    = 32767;
    public static final int     _POSIX2_BC_DIM_MAX                  = 2048;
    public static final int     LZO_SIZEOF_VOID_P                   = LZO_SIZEOF_LONG;
    public static final long    LLONG_MIN                           = (LLONG_MAX - 1L);
    public static final int     SSIZE_MAX                           = LONG_MAX;
    public static final String  __LZO_INFOSTR_OSNAME                = LZO_INFO_OS;
    public static final int     EXPR_NEST_MAX                       = _POSIX2_EXPR_NEST_MAX;
    public static final int     LONG_MIN                            = (LONG_MAX - 1);
    public static final int     BC_STRING_MAX                       = _POSIX2_BC_STRING_MAX;
    public static final int     INT_MIN                             = (INT_MAX - 1);
    public static final int     LZO_LIBC_GLIBC                      = (__GLIBC__ * 65536 + __GLIBC_MINOR__ * 256);
    public static final int     BC_DIM_MAX                          = _POSIX2_BC_DIM_MAX;
    public static final int     BC_SCALE_MAX                        = _POSIX2_BC_SCALE_MAX;
    public static final int     LZO_INT_MAX                         = LONG_MAX;
    public static final int     LZO_INT32_MAX                       = INT_MAX;
    public static final long    LZO_UINT32_MAX                      = UINT_MAX;

    public static final int     BC_BASE_MAX                         = _POSIX2_BC_BASE_MAX;                         ;

    public static final int     CHAR_MAX                            = SCHAR_MAX;                                   ;

    public static final int     PTHREAD_DESTRUCTOR_ITERATIONS       = _POSIX_THREAD_DESTRUCTOR_ITERATIONS;         ;

    public static final int     CHAR_MIN                            = SCHAR_MIN;                                   ;

    public static final String  LZO_INFO_STRING                     = LZO_INFO_ARCH;                               ;

    public static final int     LINE_MAX                            = _POSIX2_LINE_MAX;                            ;

    public static final long    LZO_UINT_MAX                        = ULONG_MAX;                                   ;

    public static native int __lzo_align_gap(Pointer _ptr, NativeLong _size);;

    public static native int __lzo_init_v2(int u1, int int1, int int2,
            int int3, int int4, int int5, int int6, int int7, int int8, int int9);

    public static native int _lzo_config_check();

    public static native Pointer _lzo_version_date();

    public static native Pointer _lzo_version_string();

    /**
     * Is the native lzo2 loaded?
     * 
     * @return true if loaded, otherwise false
     */
    public static boolean isNativeCodeLoaded()
    {
        return nativeLibraryLoaded;
    }

    public static native int lzo_adler32(int _adler, byte _buf[],
            NativeLong _len);

    public static native int lzo_crc32(int _c, byte _buf[], NativeLong _len);

    public static native IntByReference lzo_get_crc32_table();

    public static native int lzo_memcmp(Pointer _s1, Pointer _s2,
            NativeLong _len);

    public static native Pointer lzo_memcpy(Pointer _dest, Pointer _src,
            NativeLong _len);

    public static native Pointer lzo_memmove(Pointer _dest, Pointer _src,
            NativeLong _len);

    public static native Pointer lzo_memset(Pointer _s, int _c, NativeLong _len);

    public static native int lzo_version();

    public static native Pointer lzo_version_date();

    public static native Pointer lzo_version_string();

    public static native int lzo1_99_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1_compress(ByteBuffer src, NativeLong src_len,
            byte[] dst, NativeLongByReference dst_len, Pointer wrkmem);

    public static native int lzo1_decompress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1a_99_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1a_compress(ByteBuffer src, NativeLong src_len,
            ByteBuffer dst, NativeLongByReference dst_len, Pointer wrkmem);

    public static native int lzo1a_decompress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1b_1_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1b_2_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1b_3_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1b_4_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1b_5_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1b_6_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1b_7_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1b_8_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1b_9_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1b_99_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1b_999_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1b_compress(ByteBuffer src, NativeLong src_len,
            ByteBuffer dst, NativeLongByReference dst_len, Pointer wrkmem,
            int compression_level);

    public static native int lzo1b_decompress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1b_decompress_safe(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_1_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_2_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_3_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_4_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_5_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_6_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_7_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_8_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_9_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_99_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_999_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_compress(ByteBuffer src, NativeLong src_len,
            ByteBuffer dst, NativeLongByReference dst_len, Pointer wrkmem,
            int compression_level);

    public static native int lzo1c_decompress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_decompress_asm(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_decompress_asm_safe(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1c_decompress_safe(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1f_1_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1f_999_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1f_decompress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1f_decompress_asm_fast(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1f_decompress_asm_fast_safe(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1f_decompress_safe(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1x_1_11_compress(byte[] src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1x_1_12_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1x_1_15_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1x_1_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1x_999_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1x_999_compress_dict(byte in[],
            NativeLong in_len, ByteBuffer out, NativeLongByReference out_len,
            Pointer wrkmem, byte dict[], NativeLong dict_len);

    public static native int lzo1x_999_compress_level(byte in[],
            NativeLong in_len, ByteBuffer out, NativeLongByReference out_len,
            Pointer wrkmem, byte dict[], NativeLong dict_len,
            lzo_callback_t cb, int compression_level);

    public static native int lzo1x_decompress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1x_decompress_asm(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1x_decompress_asm_fast(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1x_decompress_asm_fast_safe(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1x_decompress_asm_safe(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1x_decompress_dict_safe(byte in[],
            NativeLong in_len, ByteBuffer out, NativeLongByReference out_len,
            Pointer wrkmem, byte dict[], NativeLong dict_len);

    public static native int lzo1x_decompress_safe(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1x_optimize(ByteBuffer in, NativeLong in_len,
            ByteBuffer out, NativeLongByReference out_len, Pointer wrkmem);

    public static native int lzo1y_1_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1y_999_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1y_999_compress_dict(byte in[],
            NativeLong in_len, ByteBuffer out, NativeLongByReference out_len,
            Pointer wrkmem, byte dict[], NativeLong dict_len);

    public static native int lzo1y_999_compress_level(byte in[],
            NativeLong in_len, ByteBuffer out, NativeLongByReference out_len,
            Pointer wrkmem, byte dict[], NativeLong dict_len,
            lzo_callback_t cb, int compression_level);

    public static native int lzo1y_decompress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1y_decompress_asm(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1y_decompress_asm_fast(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1y_decompress_asm_fast_safe(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1y_decompress_asm_safe(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1y_decompress_dict_safe(byte in[],
            NativeLong in_len, ByteBuffer out, NativeLongByReference out_len,
            Pointer wrkmem, byte dict[], NativeLong dict_len);

    public static native int lzo1y_decompress_safe(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1y_optimize(ByteBuffer in, NativeLong in_len,
            ByteBuffer out, NativeLongByReference out_len, Pointer wrkmem);

    public static native int lzo1z_999_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1z_999_compress_dict(byte in[],
            NativeLong in_len, ByteBuffer out, NativeLongByReference out_len,
            Pointer wrkmem, byte dict[], NativeLong dict_len);

    public static native int lzo1z_999_compress_level(byte in[],
            NativeLong in_len, ByteBuffer out, NativeLongByReference out_len,
            Pointer wrkmem, byte dict[], NativeLong dict_len,
            lzo_callback_t cb, int compression_level);

    public static native int lzo1z_decompress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo1z_decompress_dict_safe(byte in[],
            NativeLong in_len, ByteBuffer out, NativeLongByReference out_len,
            Pointer wrkmem, byte dict[], NativeLong dict_len);

    public static native int lzo1z_decompress_safe(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo2a_999_compress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo2a_decompress(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);

    public static native int lzo2a_decompress_safe(ByteBuffer src,
            NativeLong src_len, ByteBuffer dst, NativeLongByReference dst_len,
            Pointer wrkmem);
}
