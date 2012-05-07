#!/bin/bash

# Allow user to specify - this is done by packages
if [ -n "$BUILD_REVISION" ]; then
  echo $BUILD_REVISION
  exit
fi

# If we're in git, use that
BUILD_REVISION=$(git rev-parse HEAD 2>/dev/null)
if [ -n "$BUILD_REVISION" ]; then
  echo $BUILD_REVISION
  exit
fi

# Otherwise try to use the .archive-version file which
# is filled in by git exports (eg github downloads)
BIN=$(dirname ${BASH_SOURCE:-0})
BUILD_REVISION=$(cat $BIN/../.archive-version 2>/dev/null)

if [[ "$BUILD_REVISION" != *Format* ]]; then
  echo "$BUILD_REVISION"
  exit
fi

# Give up
echo "Unknown build revision"
