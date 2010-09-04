Hadoop-LZO
==========

Hadoop-LZO is a project to bring splittable LZO compression to Hadoop.  LZO is an ideal compression format for Hadoop due to its combination of speed and compression size.  However, LZO files are not natively splittable, meaning the parallelism that is the core of Hadoop is gone.  This project re-enables that parallelism with LZO compressed files, and also comes with standard utilities (input/output streams, etc) for working with LZO files.

### Origins

This project builds off the great work done at [code.google.com/p/hadoop-gpl-compression](code.google.com/p/hadoop-gpl-compression).  As of issue 41, the differences in this codebase are the following.

- it fixes a few bugs in hadoop-gpl-compression -- notably, it allows the decompressor to read small or uncompressable lzo files, and also fixes the compressor to follow the lzo standard when compressing small or uncompressible chunks.  it also fixes a number of inconsistenly caught and thrown exception cases that can occur when the lzo writer gets killed mid-stream, plus some other smaller issues (see commit log).
- it adds the ability to work with Hadoop streaming via the com.apache.hadoop.mapred.DeprecatedLzoTextInputFormat class
- it adds an easier way to index lzo files (com.hadoop.compression.lzo.LzoIndexer)
- it adds an even easier way to index lzo files, in a distributed manner (com.hadoop.compression.lzo.DistributedLzoIndexer)

### Hadoop and LZO, Together at Last

LZO is a wonderful compression scheme to use with Hadoop because it's incredibly fast, and (with a bit of work) it's splittable.  Gzip is decently fast, but cannot take advantage of Hadoop's natural map splits because it's impossible to start decompressing a gzip stream starting at a random offset in the file.  LZO's block format makes it possible to start decompressing at certain specific offsets of the file -- those that start new LZO block boundaries.  In addition to providing LZO decompression support, these classes provide an in-process indexer (com.hadoop.compression.lzo.LzoIndexer) and a map-reduce style indexer which will read a set of LZO files and output the offsets of LZO block boundaries that occur near the natural Hadoop block boundaries.  This enables a large LZO file to be split into multiple mappers and processed in parallel.  Because it is compressed, less data is read off disk, minimizing the number of IOPS required.  And LZO decompression is so fast that the CPU stays ahead of the disk read, so there is no performance impact from having to decompress data as it's read off disk.

### Building and Configuring

To get started, see [http://code.google.com/p/hadoop-gpl-compression/wiki/FAQ](http://code.google.com/p/hadoop-gpl-compression/wiki/FAQ).  This project is built exactly the same way; please follow the answer to "How do I configure Hadoop to use these classes?" on that page.

You can read more about Hadoop, LZO, and how we're using it at Twitter at [http://www.cloudera.com/blog/2009/11/17/hadoop-at-twitter-part-1-splittable-lzo-compression/](http://www.cloudera.com/blog/2009/11/17/hadoop-at-twitter-part-1-splittable-lzo-compression/).

Once the libs are built and installed, you may want to add them to the class paths and library paths.  That is, in hadoop-env.sh, set

        export HADOOP_CLASSPATH=/path/to/your/hadoop-lzo-lib.jar
        export JAVA_LIBRARY_PATH=/path/to/hadoop-lzo-native-libs:/path/to/standard-hadoop-native-libs

Note that there seems to be a bug in /path/to/hadoop/bin/hadoop; comment out the line

        JAVA_LIBRARY_PATH=''

because it keeps Hadoop from keeping the alteration you made to JAVA_LIBRARY_PATH above.  (Update: see [https://issues.apache.org/jira/browse/HADOOP-6453](https://issues.apache.org/jira/browse/HADOOP-6453)).  Make sure you restart your jobtrackers and tasktrackers after uploading and changing configs so that they take effect.

### Building and Configuring (using JNA - experimental)

Building library:

        git clone git://github.com/nandub/hadoop-lzo.git
        cd hadoop-lz
        ant clean jar

        cp build/hadoop-lzo-x.x.x.jar $HADOOP_INSTALL_DIR/lib
        cp jna.jar $HADOOP_INSTALL_DIR/lib

Configuring hadoop to use JNA.  Edit the hadoop-env.sh file:

        export HADOOP_CLASSPATH=$HADOOP_INSTALL_DIR/lib/hadoop-lzo-x.x.x.jar
        export JAVA_LIBRARY_PATH=$HADOOP_INSTALL_DIR/lib

Additional steps are needed to add entries to hadoop configuration file to register the external codecs in the codec factory. Add the following key/value pairs into hadoop-site.xml (or core-site.xml):

    <property>
        <name>io.compression.codecs</name>
        <value>org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,
               com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,
               org.apache.hadoop.io.compress.BZip2Codec,
               com.hadoop.compression.lzo.jna.LzoCodec,com.hadoop.compression.lzo.jna.LzopCodec
        </value>
    </property>
    <property>
        <name>io.compression.codec.lzo.class</name>
        <value>com.hadoop.compression.lzo.jna.LzoCodec</value>
    </property>

If you would like to use lzo to compress intermediate map output, set the following in hadoop-site.xml:

    <property>
        <name>mapred.compress.map.output</name>
        <value>true</value>
    </property>
    <property>
        <name>mapred.map.output.compression.codec</name>
        <value>com.hadoop.compression.lzo.jna.LzoCodec</value>
    </property>

Or if you are using Hadoop 0.21 or later, set the following in mapred-site.xml:

    <property>
        <name>mapreduce.map.output.compress</name>
        <value>true</value>
    </property>
    <property>
        <name>mapreduce.map.output.compress.codec</name>
        <value>com.hadoop.compression.lzo.LzoCodec</value>
    </property>

### Using Hadoop and LZO

#### Reading and Writing LZO Data
The project provides LzoInputStream and LzoOutputStream wrapping regular streams, to allow you to easily read and write compressed LZO data.  

#### Indexing LZO Files

At this point, you should also be able to use the indexer to index lzo files in Hadoop (recall: this makes them splittable, so that they can be analyzed in parallel in a mapreduce job).  Imagine that big_file.lzo is a 1 GB LZO file. You have two options:

- index it in-process via:

        hadoop jar /path/to/your/hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer big_file.lzo

- index it in a map-reduce job via:

        hadoop jar /path/to/your/hadoop-lzo.jar com.hadoop.compression.lzo.DistributedLzoIndexer big_file.lzo

Either way, after 10-20 seconds there will be a file named big_file.lzo.index.  The newly-created index file tells the LzoTextInputFormat's getSplits function how to break the LZO file into splits that can be decompressed and processed in parallel.  Alternatively, if you specify a directory instead of a filename, both indexers will recursively walk the directory structure looking for .lzo files, indexing any that do not already have corresponding .lzo.index files.

#### Running MR Jobs over Indexed Files

Now run any job, say wordcount, over the new file.  In Java-based M/R jobs, just replace any uses of TextInputFormat by LzoTextInputFormat.  In streaming jobs, add "-inputformat com.hadoop.mapred.DeprecatedLzoTextInputFormat" (streaming still uses the old APIs, and needs a class that inherits from org.apache.hadoop.mapred.InputFormat).  For Pig jobs, email me or check the pig list -- I have custom LZO loader classes that work but are not (yet) contributed back.

Note that if you forget to index an .lzo file, the job will work but will process the entire file in a single split, which will be less efficient.
