#
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

# Check to see if the install program supports -C
# If so, use "install -C" for the headers. Otherwise, every install
# updates the timestamps on the installed headers, which causes a recompilation
# of any downstream libraries.
AC_DEFUN([CHECK_INSTALL_CFLAG],[
AC_REQUIRE([AC_PROG_INSTALL])
touch foo
if $INSTALL -C foo bar; then
  INSTALL_DATA="$INSTALL_DATA -C"
fi
rm -f foo bar
])

# COMPUTE_NEEDED_DSO(LIBRARY, PREPROC_SYMBOL)
# --------------------------------------------------
# Compute the 'actual' dynamic-library used 
# for LIBRARY and set it to PREPROC_SYMBOL
AC_DEFUN([COMPUTE_NEEDED_DSO],
[
AC_CACHE_CHECK([Checking for the 'actual' dynamic-library for '-l$1'], ac_cv_libname_$1,
  [
  echo 'int main(int argc, char **argv){return 0;}' > conftest.c
  if test -z "`${CC} ${CFLAGS} ${LDFLAGS} -o conftest conftest.c -l$1 2>&1`"; then
    dnl Try otool, objdump and ldd in that order to get the dynamic library
    if test ! -z "`which otool | grep -v 'no otool'`"; then
      ac_cv_libname_$1=\"`otool -L conftest | grep $1 | sed -e 's/^[	 ]*//' -e 's/ .*//'`\";
    elif test ! -z "`which objdump | grep -v 'no objdump'`"; then
      ac_cv_libname_$1="`objdump -p conftest | grep NEEDED | grep $1 | sed 's/\W*NEEDED\W*\(.*\)\W*$/\"\1\"/'`"
    elif test ! -z "`which ldd | grep -v 'no ldd'`"; then
      ac_cv_libname_$1="`ldd conftest | grep $1 | sed 's/^[[[^A-Za-z0-9]]]*\([[[A-Za-z0-9\.]]]*\)[[[^A-Za-z0-9]]]*=>.*$/\"\1\"/'`"
    else
      AC_MSG_ERROR(Can't find either 'objdump' or 'ldd' to compute the dynamic library for '-l$1')
    fi
  else
    AC_MSG_ERROR(Can't find library for '-l$1')
  fi
  rm -f conftest*
  ]
)
AC_DEFINE_UNQUOTED($2, ${ac_cv_libname_$1}, [The 'actual' dynamic-library for '-l$1'])
])# COMPUTE_NEEDED_DSO
