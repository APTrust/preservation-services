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
        cmd: "#{bin}/redis-server",
        msg: "Redis is running on 127.0.0.1:6379"
      },
      {
        name: "minio",
        cmd: "#{bin}/minio server --quiet --address=127.0.0.1:9899 ~/tmp/minio",
        msg: "Minio is running on 127.0.0.1:9899. User/Pwd: minioadmin/minioadmin"
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
        cmd: "#{bin}/nsqd --lookupd-tcp-address=127.0.0.1:4160",
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
    cmd = "go test -p 1 #{arg}"
    puts cmd
    pid = Process.spawn(env_hash, cmd, chdir: project_root)
    Process.wait pid
    self.print_results
  end

  def run_integration_tests(arg)
    clean_test_cache
    make_test_dirs
    @all_services.each do |svc|
      start_service(svc)
    end
    self.pharos_start
    sleep(5)

    puts "Starting integration tests..."
    arg = "./..." if arg.nil?
    cmd = "go test -p 1 -tags=integration #{arg}"
    puts cmd
    pid = Process.spawn(env_hash, cmd, chdir: project_root)
    Process.wait pid
    self.print_results
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
    env['APT_SERVICES_CONFIG'] = 'test'
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
    # S3 buckets for minio
    buckets = ["preservation", "receiving", "replication", "staging"]
    buckets.each do |bucket|
      full_bucket = File.join(base, "minio", bucket)
      puts "Creating local minio bucket #{bucket}"
      FileUtils.mkdir_p full_bucket
    end
  end

  def project_root
    File.expand_path(File.join(File.dirname(__FILE__), ".."))
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

  def print_results
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
  end.parse!

  t = TestRunner.new(options)
  test_name = ARGV[0]
  if !['units', 'integration'].include?(test_name)
    t.print_help
	exit(false)
  end
  at_exit { t.stop_all_services }
  if test_name == 'units'
    t.run_unit_tests(ARGV[1])
  elsif test_name == 'integration'
    t.run_integration_tests(ARGV[1])
  end
  t.stop_all_services
end
