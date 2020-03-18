#!/bin/bash
#
# identify_file.sh
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
# Usage:
#
# identify_file.sh <url>
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
#
# ------------------------------------------------------------------------

MATCH="OK,%(info.mimetype)s,%(info.matchtype)s\n"
NOMATCH="FAIL,,%(info.matchtype)s\n"

if [ -z "$FIDO" ]
then
    FIDO=`which fido`
fi

if [ -z "$PYTHON2" ]
then
    PYTHON2=`which python2`
fi

[ -z "$FIDO" ] && echo "Can't find fido in your PATH" && exit 1
[ -z "$PYTHON2" ] && echo "Can't find python2 in your PATH" && exit 2
[ -z "$1" ] && echo "You must specify a URL" && exit 3

curl -s $1 | PYTHON2 -u $FIDO -q -matchprintf=$MATCH -nomatchprintf=$NOMATCH -
