#!/bin/sh

if [ -z "$BUILD_REVISION" ]; then
  git rev-parse HEAD
else
  echo $BUILD_REVISION
fi
