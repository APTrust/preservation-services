# Profiles

This directory contains BagIt profiles used by the validator during
ingest, and a pronom signature file (default.sig) that siegfried uses
to identify file formats.

## Siegfried Signature File

We're using a customized signature file for our Siegfried format identifier to work around Siegfried's excessively high memory consumption in the ingest format identier. That issue is logged at https://trello.com/c/5U1NXcns/719-format-identifier-uses-a-lot-of-memory.

When Siegfried tries to identify files by byte matching, it consumes the entire byte stream, keeping large chunks of data in memory. This becomes a problem when we're identifying thousands of files that may each be a few megabytes in size (or a single multi-gigabyte file).

To work around this, we've generated a custom signature file using PRONOM. Our custom file is the same as Siegfried's default.sig, except that it specifies beginning-of-file (bof) and end-of-file (eof) limits. We will read at most the first 32kb and last 8kb from each file when attempting to do byte matching.

We generated our custom signature file using roy, as described at https://github.com/richardlehane/siegfried/wiki/Building-a-signature-file-with-ROY.

We can regenerate this file at any time by doing the following:

1. Follow instructions in the link above to install roy.

2. Run `roy harvest -help` to see your siegfried home directory. You should see output like this:

```
  -home string
    	override the default home directory (default "/Users/diamond/siegfried")
```

3. Download the latest Siegfried data zip files from https://github.com/richardlehane/siegfried/releases (because `roy harvest` does not work).

4. Run `roy build -bof 32768 -eof 8096` to generate a new signature file that will read only the first 32k of data from each file it tries to identify. This generates a new default.sig file in your Siegfried home directory.

5. Copy the new default.sig file into this directoy. E.g. `cp ~/siegfried/default.sig .`

6. Run integration and e2e tests.
