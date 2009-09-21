# This file is part of Hadoop-Gpl-Compression.
#
# Hadoop-Gpl-Compression is free software: you can redistribute it
# and/or modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
#
# Hadoop-Gpl-Compression is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Hadoop-Gpl-Compression.  If not, see
# <http://www.gnu.org/licenses/>.
ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = @JNI_CPPFLAGS@ -I$(srcdir)/impl -Isrc/com/hadoop/compression/lzo
AM_LDFLAGS = @JNI_LDFLAGS@
AM_CFLAGS = -g -Wall -fPIC -O2 -m$(JVM_DATA_MODEL)

# Define the libaries that need to be built
lib_LTLIBRARIES = libgplcompression.la

# Define the sources for lib 
libgplcompression_la_SOURCES = \
	impl/lzo/LzoCompressor.c \
	impl/lzo/LzoDecompressor.c

