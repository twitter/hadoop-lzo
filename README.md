Hadoop-LZO [![Build Status](https://travis-ci.org/twitter/hadoop-lzo.png?branch=master)](https://travis-ci.org/twitter/hadoop-lzo)
==========

Hadoop-LZO is a project to bring splittable LZO compression to Hadoop.  LZO is an ideal compression format for Hadoop due to its combination of speed and compression size.  However, LZO files are not natively splittable, meaning the parallelism that is the core of Hadoop is gone.  This project re-enables that parallelism with LZO compressed files, and also comes with standard utilities (input/output streams, etc) for working with LZO files.

### Origins

This project builds off the great work done at [https://code.google.com/p/hadoop-gpl-compression](https://code.google.com/p/hadoop-gpl-compression).  As of issue 41, the differences in this codebase are the following.

- it fixes a few bugs in hadoop-gpl-compression -- notably, it allows the decompressor to read small or uncompressable lzo files, and also fixes the compressor to follow the lzo standard when compressing small or uncompressible chunks.  it also fixes a number of inconsistently caught and thrown exception cases that can occur when the lzo writer gets killed mid-stream, plus some other smaller issues (see commit log).
- it adds the ability to work with Hadoop streaming via the com.apache.hadoop.mapred.DeprecatedLzoTextInputFormat class
- it adds an easier way to index lzo files (com.hadoop.compression.lzo.LzoIndexer)
- it adds an even easier way to index lzo files, in a distributed manner (com.hadoop.compression.lzo.DistributedLzoIndexer)

### Hadoop and LZO, Together at Last

LZO is a wonderful compression scheme to use with Hadoop because it's incredibly fast, and (with a bit of work) it's splittable.  Gzip is decently fast, but cannot take advantage of Hadoop's natural map splits because it's impossible to start decompressing a gzip stream starting at a random offset in the file.  LZO's block format makes it possible to start decompressing at certain specific offsets of the file -- those that start new LZO block boundaries.  In addition to providing LZO decompression support, these classes provide an in-process indexer (com.hadoop.compression.lzo.LzoIndexer) and a map-reduce style indexer which will read a set of LZO files and output the offsets of LZO block boundaries that occur near the natural Hadoop block boundaries.  This enables a large LZO file to be split into multiple mappers and processed in parallel.  Because it is compressed, less data is read off disk, minimizing the number of IOPS required.  And LZO decompression is so fast that the CPU stays ahead of the disk read, so there is no performance impact from having to decompress data as it's read off disk.

You can read more about Hadoop, LZO, and how we're using it at Twitter at [https://www.cloudera.com/blog/2009/11/17/hadoop-at-twitter-part-1-splittable-lzo-compression/](https://www.cloudera.com/blog/2009/11/17/hadoop-at-twitter-part-1-splittable-lzo-compression/).

### Building and Configuring

To get started, see [https://code.google.com/p/hadoop-gpl-compression/wiki/FAQ](https://code.google.com/p/hadoop-gpl-compression/wiki/FAQ).  This project is built exactly the same way; please follow the answer to "How do I configure Hadoop to use these classes?" on that page, or follow the summarized version here.

You need JDK 1.6 or higher to build hadoop-lzo (1.7 or higher on Mac OS).

LZO 2.x is required, and most easily installed via the package manager on your system. If you choose to install manually for whatever reason (developer OSX machines is a common use-case) this is accomplished as follows:

1. Download the latest LZO release from https://www.oberhumer.com/opensource/lzo/
1. Configure LZO to build a shared library (required) and use a package-specific prefix (optional but recommended): `./configure --enable-shared --prefix /usr/local/lzo-2.10`
1. Build and install LZO: `make && sudo make install`
1. On Windows, you can build lzo2.dll with this command: `B\win64\vc_dll.bat`

Now let's build hadoop-lzo.

    C_INCLUDE_PATH=/usr/local/lzo-2.10/include \
    LIBRARY_PATH=/usr/local/lzo-2.10/lib \
      mvn clean package

Running tests on Windows also requires setting PATH to include the location of lzo2.dll.

    set PATH=C:\lzo-2.10;%PATH%

Additionally on Windows, the Hadoop core code requires setting HADOOP_HOME so that the tests can find winutils.exe.  If you've built Hadoop trunk in directory C:\hdc, then the following would work.

    set HADOOP_HOME=C:\hdc\hadoop-common-project\hadoop-common\target

Once the libs are built and installed, you may want to add them to the class paths and library paths.  That is, in hadoop-env.sh, set

        export HADOOP_CLASSPATH=/path/to/your/hadoop-lzo-lib.jar
        export JAVA_LIBRARY_PATH=/path/to/hadoop-lzo-native-libs:/path/to/standard-hadoop-native-libs

Note that there seems to be a bug in /path/to/hadoop/bin/hadoop; comment out the line

        JAVA_LIBRARY_PATH=''

because it keeps Hadoop from keeping the alteration you made to JAVA_LIBRARY_PATH above.  (Update: see [https://issues.apache.org/jira/browse/HADOOP-6453](https://issues.apache.org/jira/browse/HADOOP-6453)).  Make sure you restart your jobtrackers and tasktrackers after uploading and changing configs so that they take effect.

### Build Troubleshooting

The following missing LZO header error suggests LZO was installed in non-standard location and
cannot be found at build time. Double-check the environment variable C_INCLUDE_PATH is set to the
LZO include directory. For example: `C_INCLUDE_PATH=/usr/local/lzo-2.10/include`

    [exec] checking lzo/lzo2a.h presence... no
    [exec] checking for lzo/lzo2a.h... no
    [exec] configure: error: lzo headers were not found...
    [exec]                gpl-compression library needs lzo to build.
    [exec]                Please install the requisite lzo development package.

The following `Can't find library for '-llzo2'` error suggests LZO was installed to a non-standard location and cannot be located at build time. This could be one of two issues:

1. LZO was not built as a shared library. Double-check the location you installed LZO contains shared libraries (probably something like `/usr/lib64/liblzo2.so.2` on Linux, or `/usr/local/lzo-2.10/lib/liblzo2.dylib` on OSX).
1. LZO was not added to the library path. Double-check the environment varialbe LIBRARY_PATH points as the LZO lib directory (for example `LIBRARY_PATH=/usr/local/lzo-2.10/lib`).

    [exec] checking lzo/lzo2a.h usability... yes
    [exec] checking lzo/lzo2a.h presence... yes
    [exec] checking for lzo/lzo2a.h... yes
    [exec] checking Checking for the 'actual' dynamic-library for '-llzo2'... configure: error: Can't find library for '-llzo2'

The following "Native java headers not found" error indicates the Java header files are not available.

    [exec] checking jni.h presence... no
    [exec] checking for jni.h... no
    [exec] configure: error: Native java headers not found. Is $JAVA_HOME set correctly?

Header files are not available in all Java installs. Double-check you are using a JAVA_HOME that has an `include` directory. On OSX you may need to install a developer Java package.

    $ ls -d /Library/Java/JavaVirtualMachines/1.6.0_29-b11-402.jdk/Contents/Home/include
    /Library/Java/JavaVirtualMachines/1.6.0_29-b11-402.jdk/Contents/Home/include
    $ ls -d /System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home/include
    ls: /System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home/include: No such file or directory

### Maven repository

The hadoop-lzo package is available at `https://maven.twttr.com/`.

For example, if you are using `ivy`, add the repository in `ivysettings.xml`:
```xml
  <ibiblio name="twttr.com" m2compatible="true" root="https://maven.twttr.com/"/>
```

And include hadoop-lzo as a dependency:
```xml
  <dependency org="com.hadoop.gplcompression" name="hadoop-lzo" rev="0.4.17"/>
```

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

Now run any job, say wordcount, over the new file.  In Java-based M/R jobs, just replace any uses of TextInputFormat by LzoTextInputFormat.  In streaming jobs, add "-inputformat com.hadoop.mapred.DeprecatedLzoTextInputFormat" (streaming still uses the old APIs, and needs a class that inherits from org.apache.hadoop.mapred.InputFormat). Note that to use the DeprecatedLzoTextInputFormat properly with hadoop-streaming, you should also set the jobconf property `stream.map.input.ignoreKey=true`. That will replicate the behavior of the default TextInputFormat by stripping off the byte offset keys from the input lines that get piped to the mapper process. For Pig jobs, email me or check the pig list -- I have custom LZO loader classes that work but are not (yet) contributed back.

Note that if you forget to index an .lzo file, the job will work but will process the entire file in a single split, which will be less efficient.
