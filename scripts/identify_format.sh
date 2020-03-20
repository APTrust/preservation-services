#!/bin/bash
#
# identify_format.sh
#
# Use FIDO to identify file formats. For more on FIDO, see the GitHub
# repo at https://github.com/openpreserve/fido, or the wiki at
# http://wiki.opf-labs.org/display/KB/FIDO+usage+guide.
#
# NOTES:
#
# This script requires curl and FIDO. The latter can be installed with
# `pip install opf-fido`. FIDO says it runs on Python 2.7, with
# experimental support for python 3.
#
# This script expects both fido and python2 to be somewhere in your path.
#
# We're reading files from URLs, which fido does not support directly,
# but which can be done through pipes. We have to use python's -u flag
# so the interpreter treats the stream as unbuffered binary input. The
# dash at the beginning of the command tells fido to read from STDIN.
#
# Also note that we're only scanning the first half megabyte of the file
# at the specified URL, and we using FIDO's default -bufsize of 131072 bytes
# and default -container_bufsize of 524288 bytes, though we do have the
# -nocontainer flag enabled to prevent FIDO from trying to identify the
# contents of container files (like zip, tar, etc.) We should be able to
# identify containers by extension only, and we don't need to identify
# what's inside them.
#
# Usage:
#
# identify_format.sh <url> [filename]
#
# The URL param should be quoted to ensure safety.
#
# DO NOT PASS ACTUAL FILENAMES for the filename param, because many of
# our filenames are known to be unsafe. If you do pass a filename, use the
# GenericFile UUID plus the extension of the actual generic file. That is,
# you would pass this:
#
# f0d60bdf-68de-43f5-8fa0-5dc2cacc5f99.pdf
#
# And not this:
#
# test.edu/bag/data/unsafe $$ file * `rm -rf` name.pdf
#
# And yes, we do get file names like that.
#
# If FIDO does have to fall back to the extension for identification,
# all it cares about is the extension anyway, not the garbage that comes
# before it.
#
# Returns:
#
# A comma-separated list with the three fields: result, mime type, and
# match type. A successful match begins with OK, like so:
#
# OK,text/html,signature
#
# While a failed match begins with FAIL and the match type is fail:
#
# FAIL,,fail
#
# Examples:
#
#   Try to identify http://aptrust.org without specifying a file name:
#
#   identify_format.sh 'http://aptrust.org'
#
#   Try to identify the same URL with a file name:
#
#   identify_format.sh 'http://aptrust.org' 'index.html'
#
# The filename param is optional but useful. If fido can't identify the
# file by its contents, it will try to identify it by the less reliable
# file extension.
#
# The URL param may be a signed S3 URL
# ------------------------------------------------------------------------

#
# These are format strings for FIDO's output.
#
MATCH="OK,%(info.mimetype)s,%(info.matchtype)s\n"
NOMATCH="FAIL,,%(info.matchtype)s\n"

#
# If the environment doesn't already know where to find FIDO
# and Python2, see if we can figure it out ourselves.
#
if [ -z "$FIDO" ]
then
    FIDO=`which fido`
fi

if [ -z "$PYTHON2" ]
then
    PYTHON2=`which python2`
fi

#
# Fail if we don't get enough info to run
#
[ -z "$FIDO" ] && echo "Can't find fido in your PATH" && exit 1
[ -z "$PYTHON2" ] && echo "Can't find python2 in your PATH" && exit 2
[ -z "$1" ] && echo "You must specify a URL" && exit 3

#
# Make sure the URL exists
#
HEAD=`curl -s --head $1 | head -n 1`
STATUS=`echo $HEAD | cut -d$' ' -f2`
if [ "$HEAD" == "" ]
then
   >&2 echo "No response or connection refused"
   exit 4
fi

if [ "$STATUS" != "200" ]
then
    >&2 echo "Server returned status code $STATUS"
    exit 5
fi

#
# Our basic curl command includes -s to run silently (i.e. without
# printing out progress info), and -r 0-131072 to get only the first
# 128k or so of the file. 131072 is the default buffer size for FIDO.
#
CURL_CMD="curl -s -r 0-131072 $1"

#
# Our default Python command tells Python2 to run FIDO quiety (limiting
# informational output), and to use Python's -u flag for sending
# unbuffered binary input through STDIN. The -nocontainer flag tells
# FIDO not to try to identify what's inside of zip files, tarballs, and
# other containers.
#
PYTHON_CMD="$PYTHON2 -u $FIDO -q -matchprintf=$MATCH -nomatchprintf=$NOMATCH -nocontainer"

#
# Finally, run the curl command and pipe it's output to python/fido,
# with a filename only if it was provided. The trailing dash tells FIDO
# that data will be coming in through STDIN.
#
if [ -z "$2" ]
then
    $CURL_CMD | $PYTHON_CMD -
else
    $CURL_CMD | $PYTHON_CMD -filename=$2 -
fi
