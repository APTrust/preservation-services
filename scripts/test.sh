#!/usr/bin/env bash
# Run unit and integration tests for preservation-services.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
INGEST_BIN_DIR="$PROJECT_ROOT/bin/go-bin"
START_TIME=$(date +%s)

# Bash 3-compatible substitute for an associative array.
# Service names use only [a-z0-9_] so they are safe as variable-name suffixes.
PIDS_NAMES=()
pids_set() {
  local name="$1" pid="$2" n found=false
  for n in ${PIDS_NAMES[@]+"${PIDS_NAMES[@]}"}; do
    [[ "$n" == "$name" ]] && found=true && break
  done
  [[ "$found" == "false" ]] && PIDS_NAMES+=("$name")
  eval "PIDS_${name}=\${pid}"
}
pids_get() {
  eval "echo \"\${PIDS_${1}:-}\""
}

SERVICES_STOPPED=false
TEST_NAME=""
OPT_FORMATS=false
OPT_NOCLEANUP=false
ARG=""

# Determine OS-specific bin directory.
# For macOS, always use the amd64 binary regardless of platform.
# Minio's old arm64 binary sometimes crashes on M3 chips.
if [[ "$(uname)" == "Darwin" ]]; then
  BIN_DIR="$PROJECT_ROOT/bin/osx/amd64"
else
  BIN_DIR="$PROJECT_ROOT/bin/linux"
fi

log_file_path() {
  echo "$HOME/tmp/logs/${1}.log"
}

clean_test_cache() {
  echo "Deleting test cache from last run"
  go clean -testcache
  echo "Deleting old Redis data"
  rm -f "$PROJECT_ROOT/dump.rdb"
}

make_test_dirs() {
  local base="$HOME/tmp"
  # Safety check: only delete if path ends in "tmp"
  if [[ "$base" == */tmp ]]; then
    echo "Deleting $base"
    rm -rf "$base"
  fi
  local -a dirs=(bin logs minio nsq redis restore)
  for dir in "${dirs[@]}"; do
    echo "Creating $base/$dir"
    mkdir -p "$base/$dir"
  done
  # S3 buckets for minio. Ideally these would be read from the .env.test file.
  local -a buckets=(
    "preservation-or"
    "preservation-va"
    "glacier-oh"
    "glacier-or"
    "glacier-va"
    "glacier-deep-oh"
    "glacier-deep-or"
    "glacier-deep-va"
    "wasabi-or"
    "wasabi-tx"
    "wasabi-va"
    "receiving"
    "staging"
    "aptrust.receiving.test.test.edu"
    "aptrust.restore.test.test.edu"
    "aptrust.receiving.test.institution1.edu"
    "aptrust.restore.test.institution1.edu"
    "aptrust.receiving.test.institution2.edu"
    "aptrust.restore.test.institution2.edu"
    "aptrust.receiving.test.example.edu"
    "aptrust.restore.test.example.edu"
  )
  for bucket in "${buckets[@]}"; do
    echo "Creating local minio bucket $bucket"
    mkdir -p "$base/minio/$bucket"
  done
}

setup_env() {
  if [[ "$TEST_NAME" != "units" ]]; then
    if [[ -z "${REGISTRY_ROOT:-}" ]]; then
      echo "Error: Set env var REGISTRY_ROOT" >&2
      exit 1
    fi
    export REGISTRY_ROOT
  fi
  export APT_CONFIG_DIR="$PROJECT_ROOT"
  export APT_ENV=test
  if [[ "$TEST_NAME" == "e2e" ]]; then
    export APT_E2E=true
  fi
}

create_nsq_topics() {
  # Worker topics: create both topic and channel so workers can subscribe.
  local -a worker_topics=(
    "ingest01_prefetch"
    "ingest02_bag_validation"
    "ingest03_reingest_check"
    "ingest04_staging"
    "ingest05_format_identification"
    "ingest06_storage"
    "ingest07_storage_validation"
    "ingest08_record"
    "ingest09_cleanup"
    "restore_object"
    "restore_file"
    "delete_item"
    "fixity_check"
  )
  for t in "${worker_topics[@]}"; do
    local channel="${t}_worker_chan"
    curl -s -X POST "http://127.0.0.1:4151/topic/create?topic=${t}" > /dev/null || true
    curl -s -X POST "http://127.0.0.1:4151/channel/create?topic=${t}&channel=${channel}" > /dev/null || true
  done

  # E2E test topics: create topic ONLY (no channel). The e2e test checks
  # topic depth to detect completion. If a channel exists, NSQ routes
  # messages into the channel immediately and topic depth stays at zero,
  # causing the test to wait forever. Without a channel, messages accumulate
  # in the topic queue and the depth check works correctly.
  local -a e2e_topics=(
    "e2e_deletion_post_test"
    "e2e_fixity_post_test"
    "e2e_ingest_post_test"
    "e2e_reingest_post_test"
    "e2e_restoration_post_test"
  )
  for t in "${e2e_topics[@]}"; do
    curl -s -X POST "http://127.0.0.1:4151/topic/create?topic=${t}" > /dev/null || true
  done
}

start_service() {
  local name="$1"
  local chdir="$2"
  local cmd="$3"
  local msg="$4"
  local log_file
  log_file="$(log_file_path "$name")"

  (cd "$chdir" && exec env APT_ENV="${APT_ENV}" APT_E2E="${APT_E2E:-}" APT_CONFIG_DIR="${APT_CONFIG_DIR}" "$cmd") > "$log_file" 2>&1 &
  local pid=$!

  if [[ "$name" == "redis-server" ]]; then
    sleep 1
  fi

  echo ""
  echo "Started $name with command '$cmd' and pid $pid"
  echo "$msg"
  echo "Log file is $log_file"
  echo ""

  pids_set "$name" "$pid"
}

stop_service() {
  local name="$1"
  local pid="$2"

  if [[ -z "$pid" || "$pid" == "0" ]]; then
    echo "Pid for $name is zero. Can't kill that..."
    return
  fi

  if [[ "$(uname)" == "Linux" ]]; then
    stop_service_linux "$name"
    return
  fi

  echo "Stopping $name service (pid $pid)"
  kill -TERM "$pid" 2>/dev/null || {
    echo "Hmm... Couldn't kill $name."
    echo "Check system processes to see if a version"
    echo "of that process is lingering from a previous test run."
  }
}

# This function exists because on Linux, Process.spawn returns the pid of a
# short-lived parent process. We can't know the pid of the actual service,
# so we kill by name. Note this will kill ALL processes with that name.
stop_service_linux() {
  local name="$1"
  local pids
  pids=$(pidof "$name" 2>/dev/null || true)
  for pid in $pids; do
    kill -TERM "$pid" 2>/dev/null && echo "(Linux) Killed $name with pid $pid" || {
      echo "Hmm... Couldn't kill $name."
      echo "Check system processes to see if a version"
      echo "of that process is lingering from a previous test run."
    }
  done
}

stop_all_services() {
  if [[ "$SERVICES_STOPPED" == "true" ]]; then
    return
  fi
  echo "Stopping all services"
  for name in ${PIDS_NAMES[@]+"${PIDS_NAMES[@]}"}; do
    stop_service "$name" "$(pids_get "$name")"
  done
  # Kill whatever process holds port 8080 (the registry). This catches cases
  # where PID tracking missed the process or the registry was started externally.
  local registry_port_pid
  registry_port_pid=$(lsof -ti tcp:8080 2>/dev/null | head -1 || true)
  if [[ -n "$registry_port_pid" ]]; then
    echo "Stopping registry process on port 8080 (pid $registry_port_pid)"
    kill -TERM "$registry_port_pid" 2>/dev/null || true
  fi
  (cd "$PROJECT_ROOT" && docker-compose -f docker-compose-local.yml down) || true
  SERVICES_STOPPED=true
}

# Starts all ingest worker services. Pass any extra service names as arguments.
start_ingest_services() {
  local -a names=(
    "apt_delete"
    "apt_fixity"
    "ingest_pre_fetch"
    "ingest_validator"
    "reingest_manager"
    "ingest_staging_uploader"
    "ingest_format_identifier"
    "ingest_preservation_uploader"
    "ingest_preservation_verifier"
    "ingest_recorder"
    "bag_restorer"
    "file_restorer"
    "glacier_restorer"
  )
  if [[ "$OPT_NOCLEANUP" != "true" ]]; then
    names+=("ingest_cleanup")
  fi
  for svc in "$@"; do
    names+=("$svc")
  done
  for name in "${names[@]}"; do
    echo "Starting $name"
    start_service "$name" "$PROJECT_ROOT" "$INGEST_BIN_DIR/$name" "Started $name"
  done
}

build_ingest_services() {
  (cd "$PROJECT_ROOT" && bash scripts/build.sh)
}

registry_load_fixtures() {
  echo "Loading registry fixtures"
  local log_file
  log_file="$(log_file_path 'registry_fixtures')"
  (
    export APT_ENV=integration
    cd "${REGISTRY_ROOT}"
    go run loader/load_fixtures.go > "$log_file" 2>&1
  )
  echo "Registry fixtures loaded"
}

# Note: This assumes you have the registry repo source tree on your machine.
# It's on GitHub at https://github.com/APTrust/registry
registry_start() {
  if [[ -n "$(pids_get registry)" ]]; then
    return
  fi

  # Check if an existing registry process is already listening on port 8080.
  local existing_pid
  existing_pid=$(lsof -ti tcp:8080 2>/dev/null | head -1 || true)
  if [[ -n "$existing_pid" ]]; then
    echo ""
    echo "Error: Registry is already running on port 8080 (pid $existing_pid)."
    echo "Kill it with: kill $existing_pid"
    echo "Then try running this script again."
    echo
    exit 1
  fi
  registry_load_fixtures

  local log_file
  log_file="$(log_file_path 'registry')"
  local cmd="go run -tags=test registry.go"

  (
    export APT_ENV=integration
    cd "${REGISTRY_ROOT}"
    exec go run -tags=test registry.go > "$log_file" 2>&1
  ) &
  local registry_pid=$!

  # Always track the go run process so it is killed on exit.
  # go run does not forward SIGTERM to its child on macOS, so we must
  # kill go run directly; we find and kill the compiled binary separately.
  pids_set registry_gorun "$registry_pid"

  sleep 3

  # go run compiles an executable, puts it in a temp directory, and runs it
  # as a child process. Find the pid of that compiled binary.
  # Note: /var/folders is macOS-specific.
  local registry_process
  registry_process=$(ps -ef | grep registry | grep /var/folders | head -1 || true)
  local child_pid
  child_pid=$(echo "$registry_process" | awk '{print $2}')

  if [[ -n "$child_pid" && "$child_pid" =~ ^[0-9]+$ && "$child_pid" != "0" ]]; then
    pids_set registry "$child_pid"
    echo "Started Registry with command '$cmd', go run pid $registry_pid, binary pid $child_pid"
  else
    echo "Started Registry with command '$cmd' and pid $registry_pid"
  fi
}

# Runs the bucket reader once (--run-once), rather than as a long-running service.
run_bucket_reader() {
  echo "Starting bucket reader"
  local cmd="./bin/go-bin/ingest_bucket_reader --run-once"
  echo "$cmd"
  (cd "$PROJECT_ROOT" && $cmd)
}

# Initialize for integration, interactive, and end-to-end tests.
# Clears and rebuilds data directories, starts all services, creates NSQ topics.
init_for_integration() {
  clean_test_cache
  make_test_dirs
  registry_start
  sleep 8
  (cd "$PROJECT_ROOT" && docker-compose -f docker-compose-local.yml up -d)
  sleep 5
  create_nsq_topics
}

run_go_unit_tests() {
  local arg="${1:-./...}"
  [[ -z "$arg" ]] && arg="./..."
  echo "Starting unit tests..."

  # Note: -p 1 flag helps prevent Redis overwrites on Linux/Travis
  local -a cmd_args=(go test -p 1)
  if [[ "$OPT_FORMATS" == "true" ]]; then
    echo "Will run additional format identification tests"
    cmd_args+=(-tags=formats)
  fi
  cmd_args+=("$arg")

  echo "${cmd_args[*]}"
  local exit_code=0
  (cd "$PROJECT_ROOT" && "${cmd_args[@]}") || exit_code=$?
  print_results "$exit_code"
}

run_unit_tests() {
  local arg="${1:-}"
  clean_test_cache
  make_test_dirs
  (cd "$PROJECT_ROOT" && docker-compose -f docker-compose-local.yml up -d)
  run_go_unit_tests "$arg"
  # EXIT trap will stop all services
}

run_integration_tests() {
  local arg="${1:-./...}"
  [[ -z "$arg" ]] && arg="./..."
  init_for_integration
  echo "Starting integration tests..."

  local -a cmd_args=(go test -p 1 -tags=integration "$arg")
  echo "${cmd_args[*]}"
  local exit_code=0
  (cd "$PROJECT_ROOT" && "${cmd_args[@]}") || exit_code=$?
  print_results "$exit_code"
}

# TODO: Quit if an instance of Registry is already running on 8080.
# Note: Don't run apt_queue_fixity service here; it will queue fixture files.
# The e2e test queues specific items for fixity checks when ready.
run_e2e_tests() {
  build_ingest_services
  init_for_integration
  start_ingest_services "ingest_bucket_reader" "apt_queue"

  echo "Giving the workers some time to finish"
  sleep 15

  echo "Starting end-to-end tests..."
  local -a cmd_args=(go test -p 1 -timeout 6m -tags=e2e ./e2e/...)
  echo "${cmd_args[*]}"
  local exit_code=0
  (cd "$PROJECT_ROOT" && "${cmd_args[@]}") || exit_code=$?
  print_results "$exit_code"
}

run_interactive() {
  build_ingest_services
  init_for_integration
  start_ingest_services "ingest_bucket_reader" "apt_queue" "apt_queue_fixity"
  echo ">> NSQ: 'http://localhost:4171'"
  echo ">> Minio: 'http://localhost:9899' login/pwd -> minioadmin/minioadmin"
  echo ">> Registry: 'http://localhost:8080' login/pwd -> system@aptrust.org/password"
  echo ""
  echo "Push some bags to aptrust.receiving.test.test.edu"
  echo "on the local minio server, then run the bucket reader"
  echo "with this command:"
  echo ""
  echo "APT_ENV=test ./bin/go-bin/ingest_bucket_reader"
  echo "Use Control-C to shut it all down."
  while true; do
    sleep 1
  done
}

print_results() {
  local exit_code="$1"
  local end_time
  end_time=$(date +%s)
  local elapsed=$((end_time - START_TIME))
  echo ""
  echo "Elapsed time: $elapsed seconds"
  echo "Logs are in $HOME/tmp/logs"
  if [[ "$exit_code" -eq 0 ]]; then
    printf "\n\n    **** 😁 PASS 😁 ****\n\n"
  else
    printf "\n\n    **** 🤬 FAIL 🤬 ****\n\n"
    exit 1
  fi
}

print_help() {
  echo ""
  echo "APTrust Preservation Services tests"
  echo ""
  echo "Usage:"
  echo "  test.sh units                   # Run unit tests"
  echo "  test.sh units --formats         # Run unit and extra format tests"
  echo "  test.sh integration             # Run integration tests"
  echo "  test.sh integration --rebuild   # Rebuild Docker & run integration"
  echo "  test.sh e2e                     # Run end to end tests"
  echo "  test.sh interactive             # Run interactive tests"
  echo ""
  echo "To run unit tests in a single directory:"
  echo "  test.sh units ./ingest/..."
  echo "  test.sh integration ./network/..."
  echo "  test.sh integration ./network/... --rebuild"
  echo ""
  echo "Note that running integration tests also runs unit tests."
  echo "Go files are always rebuilt for testing."
  echo ""
  echo "The interactive option doesn't run any automated tests."
  echo "It spins up a whole APTrust environment on your local machine"
  echo "and lets you push bags through ingest and restoration."
  echo "Use this option to test new features and tricky bags, or to"
  echo "try to reproduce failures and errors from live environments."
  echo "You'll have to upload bags to a receiving bucket in the local"
  echo "Minio server. Registry will be running on localhost:8080."
  echo ""
}

# Parse arguments. Options (-f, -n) may appear anywhere among positional args.
while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--formats)
      OPT_FORMATS=true
      ;;
    -n|--nocleanup)
      OPT_NOCLEANUP=true
      ;;
    -*)
      echo "Unknown option: $1" >&2
      print_help
      exit 1
      ;;
    *)
      if [[ -z "$TEST_NAME" ]]; then
        TEST_NAME="$1"
      else
        ARG="$1"
      fi
      ;;
  esac
  shift
done

if [[ -z "$TEST_NAME" ]] || [[ ! "$TEST_NAME" =~ ^(units|integration|interactive|e2e)$ ]]; then
  print_help
  exit 1
fi

setup_env
trap 'stop_all_services || true' EXIT
trap 'stop_all_services || true; exit 130' INT
trap 'stop_all_services || true; exit 143' TERM

case "$TEST_NAME" in
  units)
    run_unit_tests "$ARG"
    ;;
  integration)
    run_integration_tests "$ARG"
    ;;
  interactive)
    run_interactive
    ;;
  e2e)
    run_e2e_tests
    ;;
esac
