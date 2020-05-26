#!/usr/bin/env ruby
# coding: utf-8

# Run unit and integration tests for preservation-services.

require 'fileutils'
require 'optparse'

class TestRunner

  def initialize(options)
    @options = options
    @pids = {}
    @pharos_started = false
    @services_stopped = false
    bin = self.bin_dir
    @unit_services = [
      {
        name: "redis",
        cmd: "#{bin}/redis-server --dir ~/tmp/redis/",
        msg: "Redis is running on 127.0.0.1:6379"
      },
      {
        # For localhost testing, use 'localhost' instead of '127.0.0.1'
        # because Minio signed URLs use hostname, not IP.
        name: "minio",
        cmd: "#{bin}/minio server --quiet --address=localhost:9899 ~/tmp/minio",
        msg: "Minio is running on localhost:9899. User/Pwd: minioadmin/minioadmin"
      }
    ]
    @integration_services = [
      {
        name: "nsqlookupd",
        cmd: "#{bin}/nsqlookupd",
        msg: "Started nsqlookupd at 127.0.0.1:4160"
      },
      {
        name: "nsqd",
        cmd: "#{bin}/nsqd --lookupd-tcp-address=127.0.0.1:4160 --data-path ~/tmp/nsq/",
        msg: "Started nsqd at 127.0.0.1:4151"
      },
      {
        name: "nsqdadmin",
        cmd: "#{bin}/nsqadmin --lookupd-http-address=127.0.0.1:4161",
        msg: "Started nsqadmin at 127.0.0.1:4171"
      }
    ]
    @all_services = @unit_services + @integration_services
  end

  def clean_test_cache
    puts "Deleting test cache from last run"
    `go clean -testcache`
    puts "Deleting old Redis data"
    File.delete('dump.rdb') if File.exists?('dump.rdb')
  end

  # Starts all the services we need to run ingest.
  # Param extra_services is a list of additional services
  # to start. For example, interactive tests need the bucket
  # reader to run as a service, so we pass in param
  # ['ingest_bucket_reader'].
  def ingest_service_commands(extra_services)
    ingest_services = []
    names = [
      "apt_delete",
      "ingest_pre_fetch",
      "ingest_validator",
      "reingest_manager",
      "ingest_staging_uploader",
      "ingest_format_identifier",
      "ingest_preservation_uploader",
      "ingest_preservation_verifier",
      "ingest_recorder",
      "ingest_cleanup",
    ]
    names += extra_services
    names.each do |name|
      ingest_services.push({
        name: name,
        cmd: "#{self.ingest_bin_dir}/#{name}",
        msg: "Started #{name}"})
    end
    ingest_services
  end

  def run_unit_tests(arg)
    clean_test_cache
    make_test_dirs
    @unit_services.each do |svc|
      start_service(svc)
    end
    run_go_unit_tests(arg)
    # at_exit handler will stop all services
  end

  def run_go_unit_tests(arg)
    # Note: -p 1 flag helps prevent Redis overwrites on Linux/Travis
    puts "Starting unit tests..."
    arg = "./..." if arg.nil?
    if @options[:formats]
      puts "Will run additional format identification tests"
      cmd = "go test -p 1 -tags=formats #{arg}"
    else
      cmd = "go test -p 1 #{arg}"
    end
    puts cmd
    pid = Process.spawn(env_hash, cmd, chdir: project_root)
    Process.wait pid
    self.print_results
  end

  def run_integration_tests(arg)
    init_for_integration
    puts "Starting integration tests..."
    arg = "./..." if arg.nil?
    cmd = "go test -p 1 -tags=integration #{arg}"
    puts cmd
    pid = Process.spawn(env_hash, cmd, chdir: project_root)
    Process.wait pid
    self.print_results
  end

  def run_interactive(arg)
    build_ingest_services
    init_for_integration
    start_ingest_services(["ingest_bucket_reader"])
    puts ">> NSQ: 'http://localhost:4171'"
    puts ">> Minio: 'http://localhost:9899' login/pwd -> minioadmin/minioadmin"
    puts ">> Pharos: 'http://localhost:9292' login/pwd -> system@aptrust.org"

    puts "Push some bags to aptrust.receiving.test.test.edu"
    puts "on the local minio server, then run the bucket reader"
    puts "with this command:\n"
    puts "APT_ENV=test ./bin/go-bin/ingest_bucket_reader"
    puts "Use Control-C to shut it all down."
    while true
      sleep(1)
    end
  end


  # TODO: Test both initial ingest and reingest.
  # TODO: Run end-to-end tests without "sleep". Needs reliable
  #       triggers to say when ingest is complete and when
  #       reingest is complete. Consider writing all the code
  #       in go with a manager that waits for WorkItems to complete.
  def run_e2e_tests(arg)
    build_ingest_services
    init_for_integration
    start_ingest_services
    run_bucket_reader

    puts "Giving the workers some time to finish"
    sleep(20)

    puts "Starting end-to-end tests..."
    cmd = "go test -p 1 -tags=e2e ./e2e/..."
    puts cmd
    pid = Process.spawn(env_hash, cmd, chdir: project_root)
    Process.wait pid
    self.print_results
  end


  # Initialize for integration, interactive tests, and
  # end to end tests. This clears and rebuilds data directories,
  # starts all services, and creates all NSQ topics.
  def init_for_integration
    clean_test_cache
    make_test_dirs
    # Start NSQ, Minio, Redis, and Docker/Pharos
    @all_services.each do |svc|
      start_service(svc)
    end
    self.pharos_start
    sleep(5)
    create_nsq_topics
  end

  # This runs the bucket reader once, as opposed to running it as
  # a service. Use this in integration and end-to-end (e2e) tests
  # when you want to control exactly when the bucket reader runs.
  def run_bucket_reader
    puts "Starting bucket reader"
    cmd = "./bin/go-bin/ingest_bucket_reader --run-once"
    puts cmd
    pid = Process.spawn(env_hash, cmd, chdir: project_root)
    Process.wait pid
  end

  def build_ingest_services
    build_pid = Process.spawn('ruby scripts/build.rb', chdir: project_root)
    Process.wait build_pid
  end

  def start_ingest_services(extra_services)
    self.ingest_service_commands(extra_services).each do |svc|
      puts "Starting #{svc['name']}"
      self.start_service(svc)
    end
  end

  # Create NSQ topics so that consumers don't wait around idly.
  # This speeds up e2e tests by several minutes.
  def create_nsq_topics
    topics = [
      "ingest01_prefetch",
      "ingest02_bag_validation",
      "ingest03_reingest_check",
      "ingest04_staging",
      "ingest05_format_identification",
      "ingest06_storage",
      "ingest07_storage_validation",
      "ingest08_record",
      "ingest09_cleanup",
    ]
    topics.each do |t|
      `curl -s -X POST http://127.0.0.1:4151/topic/create?topic=#{t}`
    end
  end

  def start_service(svc)
    log_file = File.join(ENV['HOME'], "tmp", "logs", "#{svc[:name]}.log")
    pid = Process.spawn(env_hash, svc[:cmd], out: log_file, err: log_file)
    Process.detach pid
    log_started(svc, pid, log_file)
	@pids[svc[:name]] = pid
  end

  def log_started(svc, pid, log_file)
    puts ""
    puts "Started #{svc[:name]} with command '#{svc[:cmd]}' and pid #{pid}"
    puts svc[:msg]
    puts "Log file is #{log_file}"
    puts ""
  end

  def stop_service(svc)
	pid = @pids[svc[:name]]
	if pid.nil? || pid == 0
	  return
	end
	puts "Stopping #{svc[:name]} service (pid #{pid})"
	begin
	  Process.kill('TERM', pid)
	rescue
	  puts "Hmm... Couldn't kill #{svc[:name]}."
      puts "Check system processes to see if a version "
      puts "of that process is lingering from a previous test run."
	end
  end

  def env_hash
	env = {}
	ENV.each{ |k,v| env[k] = v }
	#env['RBENV_VERSION'] = `cat #{@pharos_root}/.ruby-version`.chomp
	env['RAILS_ENV'] = 'integration'
    env['APT_CONFIG_DIR'] = File.expand_path(
      File.join(
        File.dirname(__FILE__),
        ".."
      ))
    env['APT_ENV'] = 'test'
	env
  end

  def make_test_dirs
    base = File.join(ENV['HOME'], "tmp")
    if base.end_with?("tmp") # So we don't delete anyone's home dir
      puts "Deleting #{base}"
    end
    FileUtils.remove_dir(base ,true)
    dirs = ["bin", "logs", "minio", "nsq", "redis", "restore"]
    dirs.each do |dir|
      full_dir = File.join(base, dir)
      puts "Creating #{full_dir}"
      FileUtils.mkdir_p full_dir
    end
    # S3 buckets for minio. We should ideally read these from the
    # .env.test file.
    buckets = [
      "preservation-or",
      "preservation-va",
      "glacier-oh",
      "glacier-or",
      "glacier-va",
      "glacier-deep-oh",
      "glacier-deep-or",
      "glacier-deep-va",
      "wasabi-or",
      "wasabi-va",
      "receiving",
      "staging",
      "aptrust.receiving.test.test.edu",
      "aptrust.restore.test.test.edu",
    ]
    buckets.each do |bucket|
      full_bucket = File.join(base, "minio", bucket)
      puts "Creating local minio bucket #{bucket}"
      FileUtils.mkdir_p full_bucket
    end
  end

  def project_root
    File.expand_path(File.join(File.dirname(__FILE__), ".."))
  end

  def ingest_bin_dir
    File.join(project_root, "bin", "go-bin")
  end

  def bin_dir
    os = (/darwin/ =~ RUBY_PLATFORM) ? "osx" : "linux"
    File.join(project_root, "bin", os)
  end

  def pharos_start
    pharos_root = ENV['PHAROS_ROOT'] || abort("Set env var PHAROS_ROOT")

    if @options[:rebuild]
      puts "Rebuilding Pharos docker container (because you said so)"
      build_pid = docker_pid = Process.spawn("make build", chdir: pharos_root)
      Process.wait build_pid
    else
      puts "Using existing Pharos container (use --rebuild if you want a new one)"
    end

    docker_start_pid = Process.spawn("make integration", chdir: pharos_root)
	Process.wait docker_start_pid

    @pharos_started = true
  end

  def pharos_stop
    pharos_root = ENV['PHAROS_ROOT'] || abort("Set env var PHAROS_ROOT")
    docker_stop_pid = Process.spawn("make integration_clean", chdir: pharos_root)
	Process.wait docker_stop_pid
    @pharos_started = false
  end

  def stop_all_services
    return if @services_stopped
    puts "Stopping all services"
    services = @all_services
    services.each do |svc|
      stop_service(svc)
    end
    self.pharos_stop if @pharos_started
    @services_stopped = true
  end

  def print_nostop_message
    puts "Services are still running because you passed command flag -n"
    puts "Pharos is at http://localhost:9292 (system@aptrust.org/password)"
    puts "Redis: http://localhost:6379 (no user or password required)"
    puts "Minio: http://localhost:9899 (minioadmin/minioadmin)"
    puts "To stop services, you'll need to kill the following processes:"
    @pids.each do |name, pid|
      puts "#{name} -> #{pid}"
    end
    puts "To stop Pharos: cd into Pharos dir and 'make integration_clean'"
  end

  def print_results
    puts "Logs are in #{File.join(ENV['HOME'], "tmp", "logs")}"
    if $?.success?
      puts "\n\n    **** 😁 PASS 😁 **** \n\n".force_encoding('utf-8')
    else
      puts "\n\n    **** 🤬 FAIL 🤬 **** \n\n".force_encoding('utf-8')
      exit(false)
    end
  end

  def print_help
    puts "\n"
    puts "APTrust Preservation Services tests\n\n"
	puts "Usage: "
    puts "  test.rb units                   # Run unit tests"
    puts "  test.rb units --formats         # Run unit and extra format tests"
    puts "  test.rb integration             # Run integration tests"
    puts "  test.rb integration --rebuild   # Rebuild Docker & run integration"
    puts "\n"
    puts "To run unit tests in a single directory:"
    puts "  test.rb units ./ingest/..."
    puts "  test.rb integration ./network/..."
    puts "  test.rb integration ./network/... --rebuild \n\n"
    puts "Note that running integration tests also runs unit tests."
    puts "Go files are always rebuilt for testing, but the Pharos"
    puts "Docker container is only rebuilt when you speficy --rebuild.\n\n"
  end

end

# TODO: Add command line args to specify whether to run unit tests
# or integration tests. For now, we're only running unit tests.
if __FILE__ == $0
  options = {}
  OptionParser.new do |opts|
    opts.on("-r", "--rebuild", "Rebuild Pharos docker container") do |r|
      options[:rebuild] = r
    end
    opts.on("-f", "--formats", "Run extra format identification tests") do |f|
      options[:formats] = f
    end
    opts.on("-n", "--nostop", "Don't stop services after tests complete") do |n|
      options[:nostop] = n
    end
  end.parse!

  t = TestRunner.new(options)
  test_name = ARGV[0]
  if !['units', 'integration', 'interactive'].include?(test_name)
    t.print_help
	exit(false)
  end
  if options[:nostop]
    at_exit { t.print_nostop_message }
  else
    at_exit { t.stop_all_services }
  end
  case test_name
  when 'units'
    t.run_unit_tests(ARGV[1])
  when 'integration'
    t.run_integration_tests(ARGV[1])
  when 'interactive'
    t.run_interactive(ARGV[1])
  when 'e2e'
    t.run_e2e_tests(ARGV[1])
  end
end
