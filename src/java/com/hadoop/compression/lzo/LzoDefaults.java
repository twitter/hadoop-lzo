package com.hadoop.compression.lzo;

/**
 * Added class to separate the code from LzoCodec and LzopCodec 
 * to get the default extensions.  
 * So instead of new LzoCodec().getDefaultExtension() it will call
 * LzoDefaults.getLzoDefaultExtension() or LzoDefaults.getLzopDefaultExtenstion() 
 * 
 * @author Fernando Ortiz
 * 
 * @see com.hadoop.compression.lzo.LzoCodec
 * @see com.hadoop.compression.lzo.LzopCodec
 * @see com.hadoop.compression.lzo.LzoIndexer
 * @see com.hadoop.compression.lzo.DistributedLzoIndexer
 */
public class LzoDefaults
{    
    /**
     * Get the default filename extension for this kind of compression.
     * @return the extension including the '.'
     */
    public static String getLzopDefaultExtension()
    {
        return ".lzo";
    }
    
    /**
     * Get the default filename extension for this kind of compression.
     * @return the extension including the '.'
     */
    public static String getLzoDefaultExtension() {
      return ".lzo_deflate";
    }
}
