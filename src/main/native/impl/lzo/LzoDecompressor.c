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

// The lzo2 library-handle
static void *liblzo2 = NULL;
// lzo2 library version
static jint liblzo2_version = 0;

#define MSG_LEN 1024

// The lzo 'decompressors'
static char* lzo_decompressors[] = {
  /** lzo1 decompressors */
  /* 0 */   "lzo1_decompress", 
  
  /** lzo1a compressors */
  /* 1 */   "lzo1a_decompress",

  /** lzo1b compressors */
  /* 2 */   "lzo1b_decompress", 
  /* 3 */   "lzo1b_decompress_safe",

  /** lzo1c compressors */
  /* 4 */   "lzo1c_decompress",
  /* 5 */   "lzo1c_decompress_safe",
  /* 6 */   "lzo1c_decompress_asm",
  /* 7 */   "lzo1c_decompress_asm_safe",
  
  /** lzo1f compressors */
  /* 8 */   "lzo1f_decompress",
  /* 9 */   "lzo1f_decompress_safe",
  /* 10 */  "lzo1f_decompress_asm_fast",
  /* 11 */  "lzo1f_decompress_asm_fast_safe",

  /** lzo1x compressors */
  /* 12 */  "lzo1x_decompress",
  /* 13 */  "lzo1x_decompress_safe",
  /* 14 */  "lzo1x_decompress_asm",
  /* 15 */  "lzo1x_decompress_asm_safe",
  /* 16 */  "lzo1x_decompress_asm_fast",
  /* 17 */  "lzo1x_decompress_asm_fast_safe"
  
  /** lzo1y compressors */
  /* 18 */  "lzo1y_decompress",
  /* 19 */  "lzo1y_decompress_safe",
  /* 20 */  "lzo1y_decompress_asm",
  /* 21 */  "lzo1y_decompress_asm_safe",
  /* 22 */  "lzo1y_decompress_asm_fast",
  /* 23 */  "lzo1y_decompress_asm_fast_safe",

  /** lzo1z compressors */
  /* 24 */  "lzo1z_decompress", 
  /* 25 */  "lzo1z_decompress_safe",

  /** lzo2a compressors */
  /* 26 */  "lzo2a_decompress",
  /* 27 */  "lzo2a_decompress_safe"
};

static jfieldID LzoDecompressor_clazz;
static jfieldID LzoDecompressor_finished;
static jfieldID LzoDecompressor_compressedDirectBuf;
static jfieldID LzoDecompressor_compressedDirectBufLen;
static jfieldID LzoDecompressor_uncompressedDirectBuf;
static jfieldID LzoDecompressor_directBufferSize;
static jfieldID LzoDecompressor_lzoDecompressor;

JNIEXPORT void JNICALL
Java_com_hadoop_compression_lzo_LzoDecompressor_initIDs(
	JNIEnv *env, jclass class
	) {
  void* lzo_version_ptr = NULL;

#ifdef UNIX
	// Load liblzo2.so
	liblzo2 = dlopen(HADOOP_LZO_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
	if (!liblzo2) {
	  char* msg = (char*)malloc(1000);
	  snprintf(msg, 1000, "%s (%s)!", "Cannot load " HADOOP_LZO_LIBRARY, dlerror());
	  THROW(env, "java/lang/UnsatisfiedLinkError", msg);
    free(msg);
	  return;
	}
#endif

#ifdef WINDOWS
  liblzo2 = LoadLibrary(HADOOP_LZO_LIBRARY);
  if (!liblzo2) {
    THROW(env, "java/lang/UnsatisfiedLinkError", "Cannot load lzo2.dll");
    return;
  }
#endif
    
  LzoDecompressor_clazz = (*env)->GetStaticFieldID(env, class, "clazz", 
                                                   "Ljava/lang/Class;");
  LzoDecompressor_finished = (*env)->GetFieldID(env, class, "finished", "Z");
  LzoDecompressor_compressedDirectBuf = (*env)->GetFieldID(env, class, 
                                                "compressedDirectBuf", 
                                                "Ljava/nio/Buffer;");
  LzoDecompressor_compressedDirectBufLen = (*env)->GetFieldID(env, class, 
                                                    "compressedDirectBufLen", "I");
  LzoDecompressor_uncompressedDirectBuf = (*env)->GetFieldID(env, class, 
                                                  "uncompressedDirectBuf", 
                                                  "Ljava/nio/Buffer;");
  LzoDecompressor_directBufferSize = (*env)->GetFieldID(env, class, 
                                              "directBufferSize", "I");
  LzoDecompressor_lzoDecompressor = (*env)->GetFieldID(env, class,
                                              "lzoDecompressor", "J");

  // record lzo library version
#ifdef UNIX
  LOAD_DYNAMIC_SYMBOL(lzo_version_ptr, env, liblzo2, "lzo_version");
#endif

#ifdef WINDOWS
  LOAD_DYNAMIC_SYMBOL(lzo_version_t, lzo_version_ptr, env, liblzo2,
    "lzo_version");
#endif

  liblzo2_version = (NULL == lzo_version_ptr) ? 0
    : (jint) ((lzo_version_t)lzo_version_ptr)();
}

JNIEXPORT void JNICALL
Java_com_hadoop_compression_lzo_LzoDecompressor_init(
  JNIEnv *env, jobject this, jint decompressor 
  ) {
  void *lzo_init_func_ptr = NULL;
  lzo_init_t lzo_init_function = NULL;
  void *decompressor_func_ptr = NULL;
  int rv = 0;
  const char *lzo_decompressor_function = lzo_decompressors[decompressor];
 
  // Locate the requisite symbols from liblzo2.so

  // Initialize the lzo library 

#ifdef UNIX
  dlerror();                                 // Clear any existing error
  LOAD_DYNAMIC_SYMBOL(lzo_init_func_ptr, env, liblzo2, "__lzo_init_v2");
#endif

#ifdef WINDOWS
  LOAD_DYNAMIC_SYMBOL(lzo_init_t, lzo_init_func_ptr, env, liblzo2, "__lzo_init_v2");
#endif

  lzo_init_function = (lzo_init_t)(lzo_init_func_ptr);
  rv = lzo_init_function(LZO_VERSION, (int)sizeof(short), (int)sizeof(int), 
              (int)sizeof(long), (int)sizeof(lzo_uint32), (int)sizeof(lzo_uint), 
              (int)lzo_sizeof_dict_t, (int)sizeof(char*), (int)sizeof(lzo_voidp),
              (int)sizeof(lzo_callback_t));
  if (rv != LZO_E_OK) {
    THROW(env, "Ljava/lang/InternalError", "Could not initialize lzo library!");
    return;
  }
  
  // Save the decompressor-function into LzoDecompressor_lzoDecompressor
#ifdef UNIX
  LOAD_DYNAMIC_SYMBOL(decompressor_func_ptr, env, liblzo2,
      lzo_decompressor_function);
#endif

#ifdef WINDOWS
  LOAD_DYNAMIC_SYMBOL(void *, decompressor_func_ptr, env, liblzo2,
      lzo_decompressor_function);
#endif

  (*env)->SetLongField(env, this, LzoDecompressor_lzoDecompressor,
                       JLONG(decompressor_func_ptr));

  return;
}

JNIEXPORT jint JNICALL
Java_com_hadoop_compression_lzo_LzoDecompressor_getLzoLibraryVersion(
    JNIEnv* env, jclass class) {
  return liblzo2_version;
}

JNIEXPORT jint JNICALL
Java_com_hadoop_compression_lzo_LzoDecompressor_decompressBytesDirect(
	JNIEnv *env, jobject this, jint decompressor
	) {
  jobject clazz = NULL;
  jobject compressed_direct_buf = NULL;
  lzo_uint compressed_direct_buf_len = 0;
  jobject uncompressed_direct_buf = NULL;
  lzo_uint uncompressed_direct_buf_len = 0;
  jlong lzo_decompressor_funcptr = 0;
  lzo_bytep uncompressed_bytes = NULL;
  lzo_bytep compressed_bytes = NULL;
  lzo_uint no_uncompressed_bytes = 0;
  lzo_decompress_t fptr = NULL;
  int rv = 0;
  char exception_msg[MSG_LEN];
  const char *lzo_decompressor_function = lzo_decompressors[decompressor];

	// Get members of LzoDecompressor
	clazz = (*env)->GetStaticObjectField(env, this, 
	                                             LzoDecompressor_clazz);
	compressed_direct_buf = (*env)->GetObjectField(env, this,
                                              LzoDecompressor_compressedDirectBuf);
	compressed_direct_buf_len = (*env)->GetIntField(env, this, 
                        		  							LzoDecompressor_compressedDirectBufLen);

	uncompressed_direct_buf = (*env)->GetObjectField(env, this, 
                            								  LzoDecompressor_uncompressedDirectBuf);
	uncompressed_direct_buf_len = (*env)->GetIntField(env, this,
                                                LzoDecompressor_directBufferSize);

  lzo_decompressor_funcptr = (*env)->GetLongField(env, this,
                                              LzoDecompressor_lzoDecompressor);

    // Get the input direct buffer
    LOCK_CLASS(env, clazz, "LzoDecompressor");
	uncompressed_bytes = (*env)->GetDirectBufferAddress(env, 
											                    uncompressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "LzoDecompressor");
    
 	if (uncompressed_bytes == 0) {
    return (jint)0;
	}
	
    // Get the output direct buffer
    LOCK_CLASS(env, clazz, "LzoDecompressor");
	compressed_bytes = (*env)->GetDirectBufferAddress(env, 
										                    compressed_direct_buf);
    UNLOCK_CLASS(env, clazz, "LzoDecompressor");

  if (compressed_bytes == 0) {
		return (jint)0;
	}
	
	// Decompress
  no_uncompressed_bytes = uncompressed_direct_buf_len;
  fptr = (lzo_decompress_t) FUNC_PTR(lzo_decompressor_funcptr);
  rv = fptr(compressed_bytes, compressed_direct_buf_len, uncompressed_bytes,
    &no_uncompressed_bytes, NULL); 

  if (rv == LZO_E_OK) {
    // lzo decompresses all input data
    (*env)->SetIntField(env, this, LzoDecompressor_compressedDirectBufLen, 0);
  } else {
#ifdef UNIX
    snprintf(exception_msg, MSG_LEN, "%s returned: %d", 
              lzo_decompressor_function, rv);
#endif

#ifdef WINDOWS
    _snprintf_s(exception_msg, MSG_LEN, _TRUNCATE, "%s returned: %d",
      lzo_decompressor_function, rv);
#endif

    THROW(env, "java/lang/InternalError", exception_msg);
  }
  
  return (jint)no_uncompressed_bytes;
}

/**
 * vim: sw=2: ts=2: et:
 */

