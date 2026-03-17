#!/usr/bin/env bash
#
# This script builds the binaries for all of the preservation
# services workers.


# Next line causes script to abort on the first failed command.
set -euo pipefail

# SCRIPT_DIR is the directory that contains this script.
#
# APPS_DIR points to the directory containing the "main"
# go files for each app that we're going to build.
#
# OUTPUT_DIR will contain the compiled binaries
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APPS_DIR="$(cd "$SCRIPT_DIR/../apps" && pwd)"
OUTPUT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/bin/go-bin"

# SOURCES lists the main go files for each app we're going
# to compile.
SOURCES=(
  "apt_delete/apt_delete.go"
  "apt_fixity/apt_fixity.go"
  "apt_queue/apt_queue.go"
  "apt_queue_fixity/apt_queue_fixity.go"
  "bag_restorer/bag_restorer.go"
  "file_restorer/file_restorer.go"
  "glacier_restorer/glacier_restorer.go"
  "ingest_pre_fetch/ingest_pre_fetch.go"
  "ingest_validator/ingest_validator.go"
  "reingest_manager/reingest_manager.go"
  "ingest_staging_uploader/ingest_staging_uploader.go"
  "ingest_format_identifier/ingest_format_identifier.go"
  "ingest_preservation_uploader/ingest_preservation_uploader.go"
  "ingest_preservation_verifier/ingest_preservation_verifier.go"
  "ingest_recorder/ingest_recorder.go"
  "ingest_cleanup/ingest_cleanup.go"
  "ingest_bucket_reader/ingest_bucket_reader.go"
)

build() {
  local source="$1"
  local dir_name="${source%%/*}"
  local file_name="${source##*/}"
  local exe_name="${file_name%.go}"
  local cmd="go build -o ${OUTPUT_DIR}/${exe_name} ${file_name}"
  local source_dir="${APPS_DIR}/${dir_name}"

  echo "$cmd"
  (cd "$source_dir" && $cmd)
}

build_all() {
  mkdir -p "$OUTPUT_DIR"
  for source in "${SOURCES[@]}"; do
    build "$source"
  done
  echo "Binaries are in ${OUTPUT_DIR}"
}

build_all
