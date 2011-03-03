#!/bin/bash

DIR=$(dirname $BASH_SOURCE)
cd $DIR

aclocal -Im4
libtoolize --automake --copy
autoconf
automake -ac --copy --add-missing
