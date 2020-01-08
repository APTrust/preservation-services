#!/usr/bin/env ruby

# Run unit and integration tests for preservation-services.

require 'fileutils'

class TestRunner

  def initialize
    @pids = {}
    bin = self.bin_dir
    @unit_test_services = [
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
    @integration_test_services = [
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
      },
      {
        name: "pharos",
        cmd: "",
        msg: "To be implemented..."
      }
    ]
  end

  def run_unit_tests
    puts "Deleting test cache from last run"
    `go clean -testcache`
    make_test_dirs
    @unit_test_services.each do |svc|
      start_service(svc)
    end
    run_go_unit_tests
    # at_exit handler will stop all services
  end

  def run_go_unit_tests
    puts "Starting go unit tests"
    pid = Process.spawn(env_hash, "go test ./...", chdir: project_root)
    Process.wait pid
  end

  def run_integration_tests
    puts "You're ahead of your time, my friend. Integration tests "
    puts "have not even been written yet. Try the unit tests."
    print_help
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
	  puts "No need to stop #{svc[:name]}: not running."
	end
  end

  def env_hash
	env = {}
	ENV.each{ |k,v| env[k] = v }
	#env['RBENV_VERSION'] = `cat #{@pharos_root}/.ruby-version`.chomp
	env['RAILS_ENV'] = 'integration'
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

  def stop_all_services
    puts "Stopping all services"
    services = @unit_test_services.concat(@integration_test_services)
    services.each do |svc|
      stop_service(svc)
    end
  end

  def print_help
	puts "Usage: "
    puts "       test.rb units        # Run unit tests"
    puts "       test.rb integration  # Run integration tests \n"
  end

end

# TODO: Add command line args to specify whether to run unit tests
# or integration tests. For now, we're only running unit tests.
if __FILE__ == $0
  t = TestRunner.new
  test_name = ARGV[0]
  if !['units', 'integration'].include?(test_name)
    t.print_help
	exit(false)
  end
  if test_name == 'units'
    t.run_unit_tests
  elsif test_name == 'integration'
    t.run_integration_tests
  end
  at_exit { t.stop_all_services }
end
