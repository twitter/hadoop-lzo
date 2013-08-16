/*
 * This file is part of Hadoop-Gpl-Compression.
 *
 * Hadoop-Gpl-Compression is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Hadoop-Gpl-Compression is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Hadoop-Gpl-Compression.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

#include "gpl-compression.h"
#include "lzo.h"
#include <stdlib.h>
#include <string.h>

// The lzo2 library-handle
static void *liblzo2 = NULL;
// lzo2 library version
static jint liblzo2_version = 0;

// The lzo 'compressors'
typedef struct {
  const char *function;           // The compression function
  int wrkmem;                     // The 'working memory' needed
  int compression_level;          // Compression level if required;
                                  // else UNDEFINED_COMPRESSION_LEVEL
} lzo_compressor;

#define UNDEFINED_COMPRESSION_LEVEL -999

// Default compression level used when user supplies no value.
static lzo_compressor lzo_compressors[] = {
  /** lzo1 compressors */
  /* 0 */   {"lzo1_compress", LZO1_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},
  /* 1 */   {"lzo1_99_compress", LZO1_99_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},

  /** lzo1a compressors */
  /* 2 */   {"lzo1a_compress", LZO1A_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},
  /* 3 */   {"lzo1a_99_compress", LZO1A_99_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},

  /** lzo1b compressors */
  /* 4 */   {"lzo1b_compress", LZO1B_MEM_COMPRESS, LZO1B_DEFAULT_COMPRESSION}, 
  /* 5 */   {"lzo1b_compress", LZO1B_MEM_COMPRESS, LZO1B_BEST_SPEED}, 
  /* 6 */   {"lzo1b_compress", LZO1B_MEM_COMPRESS, LZO1B_BEST_COMPRESSION}, 
  /* 7 */   {"lzo1b_1_compress", LZO1B_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 8 */   {"lzo1b_2_compress", LZO1B_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 9 */   {"lzo1b_3_compress", LZO1B_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 10 */  {"lzo1b_4_compress", LZO1B_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 11 */  {"lzo1b_5_compress", LZO1B_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 12 */  {"lzo1b_6_compress", LZO1B_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 13 */  {"lzo1b_7_compress", LZO1B_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 14 */  {"lzo1b_8_compress", LZO1B_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 15 */  {"lzo1b_9_compress", LZO1B_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 16 */  {"lzo1b_99_compress", LZO1B_99_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 17 */  {"lzo1b_999_compress", LZO1B_999_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  
  /** lzo1c compressors */
  /* 18 */  {"lzo1c_compress", LZO1C_MEM_COMPRESS, LZO1C_DEFAULT_COMPRESSION}, 
  /* 19 */  {"lzo1c_compress", LZO1C_MEM_COMPRESS, LZO1C_BEST_SPEED}, 
  /* 20 */  {"lzo1c_compress", LZO1C_MEM_COMPRESS, LZO1C_BEST_COMPRESSION}, 
  /* 21 */  {"lzo1c_1_compress", LZO1C_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 22 */  {"lzo1c_2_compress", LZO1C_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 23 */  {"lzo1c_3_compress", LZO1C_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 24 */  {"lzo1c_4_compress", LZO1C_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 25 */  {"lzo1c_5_compress", LZO1C_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 26 */  {"lzo1c_6_compress", LZO1C_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 27 */  {"lzo1c_7_compress", LZO1C_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 28 */  {"lzo1c_8_compress", LZO1C_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 29 */  {"lzo1c_9_compress", LZO1C_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 30 */  {"lzo1c_99_compress", LZO1C_99_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  /* 31 */  {"lzo1c_999_compress", LZO1C_999_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL}, 
  
  /** lzo1f compressors */
  /* 32 */  {"lzo1f_1_compress", LZO1F_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},
  /* 33 */  {"lzo1f_999_compress", LZO1F_999_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},

  /** lzo1x compressors */
  /* 34 */  {"lzo1x_1_compress", LZO1X_1_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},
  /* 35 */  {"lzo1x_11_compress", LZO1X_1_11_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},
  /* 36 */  {"lzo1x_12_compress", LZO1X_1_12_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},
  /* 37 */  {"lzo1x_15_compress", LZO1X_1_15_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},
  /* 38 */  {"lzo1x_999_compress", LZO1X_999_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},

  /** lzo1y compressors */
  /* 39 */  {"lzo1y_1_compress", LZO1Y_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},
  /* 40 */  {"lzo1y_999_compress", LZO1Y_999_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},

  /** lzo1z compressors */
  /* 41 */  {"lzo1z_999_compress", LZO1Z_999_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},

  /** lzo2a compressors */
  /* 42 */  {"lzo2a_999_compress", LZO2A_999_MEM_COMPRESS, UNDEFINED_COMPRESSION_LEVEL},
};

// The second lzo* compressor prototype - this really should be in lzoconf.h!
typedef int
(__LZO_CDECL *lzo_compress2_t)   ( const lzo_bytep src, lzo_uint  src_len,
                                  lzo_bytep dst, lzo_uintp dst_len,
                                  lzo_voidp wrkmem, int compression_level );

static jfieldID LzoCompressor_clazz;
static jfieldID LzoCompressor_finish;
static jfieldID LzoCompressor_finished;
static jfieldID LzoCompressor_uncompressedDirectBuf;
static jfieldID LzoCompressor_uncompressedDirectBufLen;
static jfieldID LzoCompressor_compressedDirectBuf;
static jfieldID LzoCompressor_directBufferSize;
static jfieldID LzoCompressor_lzoCompressor;
static jfieldID LzoCompressor_lzoCompressionLevel;
static jfieldID LzoCompressor_workingMemoryBufLen;
static jfieldID LzoCompressor_workingMemoryBuf;

JNIEXPORT void JNICALL
Java_com_hadoop_compression_lzo_LzoCompressor_initIDs(
	JNIEnv *env, jclass class
	) {
	// Load liblzo2.so
	liblzo2 = dlopen(HADOOP_LZO_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
	if (!liblzo2) {
	  char* msg = (char*)malloc(1000);
	  snprintf(msg, 1000, "%s (%s)!", "Cannot load " HADOOP_LZO_LIBRARY, dlerror());
	  THROW(env, "java/lang/UnsatisfiedLinkError", msg);
	  return;
	}
    
  LzoCompressor_clazz = (*env)->GetStaticFieldID(env, class, "clazz", 
                                                 "Ljava/lang/Class;");
  LzoCompressor_finish = (*env)->GetFieldID(env, class, "finish", "Z");
  LzoCompressor_finished = (*env)->GetFieldID(env, class, "finished", "Z");
  LzoCompressor_uncompressedDirectBuf = (*env)->GetFieldID(env, class, 
                                                    "uncompressedDirectBuf", 
                                                    "Ljava/nio/ByteBuffer;");
  LzoCompressor_uncompressedDirectBufLen = (*env)->GetFieldID(env, class, 
                                            "uncompressedDirectBufLen", "I");
  LzoCompressor_compressedDirectBuf = (*env)->GetFieldID(env, class, 
                                                        "compressedDirectBuf",
                                                        "Ljava/nio/ByteBuffer;");
  LzoCompressor_directBufferSize = (*env)->GetFieldID(env, class, 
                                            "directBufferSize", "I");
  LzoCompressor_lzoCompressor = (*env)->GetFieldID(env, class, 
                                          "lzoCompressor", "J");
  LzoCompressor_lzoCompressionLevel = (*env)->GetFieldID(env, class,
                                                "lzoCompressionLevel", "I");
  LzoCompressor_workingMemoryBufLen = (*env)->GetFieldID(env, class,
                                                "workingMemoryBufLen", "I");
  LzoCompressor_workingMemoryBuf = (*env)->GetFieldID(env, class, 
                                              "workingMemoryBuf", 
                                              "Ljava/nio/ByteBuffer;");

  // record lzo library version
  void* lzo_version_ptr = NULL;
  LOAD_DYNAMIC_SYMBOL(lzo_version_ptr, env, liblzo2, "lzo_version");
  liblzo2_version = (NULL == lzo_version_ptr) ? 0
    : (jint) ((unsigned (__LZO_CDECL *)())lzo_version_ptr)();
}

JNIEXPORT void JNICALL
Java_com_hadoop_compression_lzo_LzoCompressor_init(
  JNIEnv *env, jobject this, jint compressor 
  ) {
  const char *lzo_compressor_function = lzo_compressors[compressor].function;
 
  // Locate the requisite symbols from liblzo2.so
  dlerror();                                 // Clear any existing error

  // Initialize the lzo library 
  void *lzo_init_func_ptr = NULL;
  typedef int (__LZO_CDECL *lzo_init_t) (unsigned,int,int,int,int,int,int,int,int,int);
  LOAD_DYNAMIC_SYMBOL(lzo_init_func_ptr, env, liblzo2, "__lzo_init_v2");
  lzo_init_t lzo_init_function = (lzo_init_t)(lzo_init_func_ptr);
  int rv = lzo_init_function(LZO_VERSION, (int)sizeof(short), (int)sizeof(int), 
              (int)sizeof(long), (int)sizeof(lzo_uint32), (int)sizeof(lzo_uint), 
              (int)lzo_sizeof_dict_t, (int)sizeof(char*), (int)sizeof(lzo_voidp),
              (int)sizeof(lzo_callback_t));
  if (rv != LZO_E_OK) {
    THROW(env, "Ljava/lang/InternalError", "Could not initialize lzo library!");
    return;
  }
  
  // Save the compressor-function into LzoCompressor_lzoCompressor
  void *compressor_func_ptr = NULL;
  LOAD_DYNAMIC_SYMBOL(compressor_func_ptr, env, liblzo2, lzo_compressor_function);
  (*env)->SetLongField(env, this, LzoCompressor_lzoCompressor,
                       JLONG(compressor_func_ptr));
  
  // Save the compressor-function into LzoCompressor_lzoCompressor
  (*env)->SetIntField(env, this, LzoCompressor_workingMemoryBufLen,
                      lzo_compressors[compressor].wrkmem);
  return;
}

JNIEXPORT jint JNICALL
Java_com_hadoop_compression_lzo_LzoCompressor_getLzoLibraryVersion(
    JNIEnv* env, jclass class) {
  return liblzo2_version;
}

JNIEXPORT jint JNICALL
Java_com_hadoop_compression_lzo_LzoCompressor_compressBytesDirect(
  JNIEnv *env, jobject this, jint compressor 
	) {
  const char *lzo_compressor_function = lzo_compressors[compressor].function;

	// Get members of LzoCompressor
    jobject clazz = (*env)->GetStaticObjectField(env, this, 
                                                 LzoCompressor_clazz);
	jobject uncompressed_direct_buf = (*env)->GetObjectField(env, this, 
									                    LzoCompressor_uncompressedDirectBuf);
	lzo_uint uncompressed_direct_buf_len = (*env)->GetIntField(env, this, 
									                  LzoCompressor_uncompressedDirectBufLen);

	jobject compressed_direct_buf = (*env)->GetObjectField(env, this, 
									                        LzoCompressor_compressedDirectBuf);
	lzo_uint compressed_direct_buf_len = (*env)->GetIntField(env, this, 
									                            LzoCompressor_directBufferSize);

  // Prefer the user defined compression level.
  int compression_level = (*env)->GetIntField(env, this,
      LzoCompressor_lzoCompressionLevel);
  if (UNDEFINED_COMPRESSION_LEVEL == compression_level) {
    compression_level = lzo_compressors[compressor].compression_level;
  }

	jobject working_memory_buf = (*env)->GetObjectField(env, this, 
									                      LzoCompressor_workingMemoryBuf);

  jlong lzo_compressor_funcptr = (*env)->GetLongField(env, this,
                  LzoCompressor_lzoCompressor);

    // Get the input direct buffer
    LOCK_CLASS(env, clazz, "LzoCompressor");
	lzo_bytep uncompressed_bytes = (*env)->GetDirectBufferAddress(env, 
                                            uncompressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "LzoCompressor");
    
  if (uncompressed_bytes == 0) {
    	return (jint)0;
	}
	
    // Get the output direct buffer
    LOCK_CLASS(env, clazz, "LzoCompressor");
	lzo_bytep compressed_bytes = (*env)->GetDirectBufferAddress(env, 
                                            compressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "LzoCompressor");
    
  if (compressed_bytes == 0) {
		return (jint)0;
	}
	
    // Get the working-memory direct buffer
    LOCK_CLASS(env, clazz, "LzoCompressor");
    lzo_voidp workmem = (*env)->GetDirectBufferAddress(env, working_memory_buf);
    UNLOCK_CLASS(env, clazz, "LzoCompressor");
    
  if (workmem == 0) {
    return (jint)0;
  }
  
	// Compress
  lzo_uint no_compressed_bytes = compressed_direct_buf_len;
	int rv = 0;
  if (compression_level == UNDEFINED_COMPRESSION_LEVEL) {
    lzo_compress_t fptr = (lzo_compress_t) FUNC_PTR(lzo_compressor_funcptr);
    rv = fptr(uncompressed_bytes, uncompressed_direct_buf_len,
              compressed_bytes, &no_compressed_bytes, 
              workmem);
  } else if (strstr(lzo_compressor_function, "lzo1x_999")
             || strstr(lzo_compressor_function, "lzo1y_999")) {
    // Compression levels are only available in these codecs.
    rv = lzo1x_999_compress_level(uncompressed_bytes, uncompressed_direct_buf_len,
                                  compressed_bytes, &no_compressed_bytes,
                                  workmem, NULL, 0, 0, compression_level);
  } else {
    lzo_compress2_t fptr = (lzo_compress2_t) FUNC_PTR(lzo_compressor_funcptr);
    rv = fptr(uncompressed_bytes, uncompressed_direct_buf_len,
              compressed_bytes, &no_compressed_bytes, 
              workmem, compression_level); 
  }

  if (rv == LZO_E_OK) {
    // lzo compresses all input data
    (*env)->SetIntField(env, this, 
                LzoCompressor_uncompressedDirectBufLen, 0);
  } else {
    const int msg_len = 32;
    char exception_msg[msg_len];
    snprintf(exception_msg, msg_len, "%s returned: %d", lzo_compressor_function, rv);
    THROW(env, "java/lang/InternalError", exception_msg);
  }

  return (jint)no_compressed_bytes;
}

/**
 * vim: sw=2: ts=2: et:
 */

