FROM centos:7.6.1810

ARG gcc_version=4.9-2016.02
ENV LZO_VERSION=2.10
ENV GCC_VERSION $gcc_version

# Install requirements
RUN yum install -y wget tar git make java-1.8.0-openjdk-devel maven which gcc

# Install aarch64 gcc toolchain
RUN set -x && \
    wget https://releases.linaro.org/components/toolchain/binaries/$GCC_VERSION/aarch64-linux-gnu/gcc-linaro-$GCC_VERSION-x86_64_aarch64-linux-gnu.tar.xz && \
    tar xvf gcc-linaro-$GCC_VERSION-x86_64_aarch64-linux-gnu.tar.xz

# Settting paths
ENV PATH="/gcc-linaro-$GCC_VERSION-x86_64_aarch64-linux-gnu/bin:${PATH}"
ENV JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk/"

# Checkout and build lzo for aarch64
ADD https://www.oberhumer.com/opensource/lzo/download/lzo-$LZO_VERSION.tar.gz ./
RUN tar -xvf lzo-$LZO_VERSION.tar.gz && \
    cd lzo-$LZO_VERSION/ && \
    ./configure --enable-shared --prefix /usr/local/lzo-$LZO_VERSION-arm --host=aarch64-linux-gnu && \
    make clean && \
    make install

# Installing cross compiled lzo on aarch64 gcc toolchain
RUN cp -r  /usr/local/lzo-$LZO_VERSION-arm/lib/* /gcc-linaro-$GCC_VERSION-x86_64_aarch64-linux-gnu/aarch64-linux-gnu/lib64/ && \
    cp -r  /usr/local/lzo-$LZO_VERSION-arm/include/* /gcc-linaro-$GCC_VERSION-x86_64_aarch64-linux-gnu/aarch64-linux-gnu/include/

# Build lzo for x86
RUN cd /lzo-$LZO_VERSION/ && \
    ./configure --enable-shared --prefix /usr/local/lzo-$LZO_VERSION && \
    make clean && \
    make install

# Settting lzo paths
ENV C_INCLUDE_PATH="/usr/local/lzo-$LZO_VERSION/include"
ENV LIBRARY_PATH="/usr/local/lzo-$LZO_VERSION/lib"
