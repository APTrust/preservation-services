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

  def run_unit_tests()
    make_test_dirs
    @unit_test_services.each do |svc|
      start_service(svc)
    end
    run_go_unit_tests
    # at_exit handler will stop all services
  end

  def run_go_unit_tests()
    puts "Starting go unit tests"
    pid = Process.spawn(env_hash, "go test ./...", chdir: project_root)
    Process.wait pid
  end

  def start_service(svc)
    log_file = File.join(ENV['HOME'], "tmp", "logs", "#{svc[:name]}.log")
    pid = Process.spawn(env_hash, svc[:cmd], out: log_file, err: log_file)
    Process.detach pid
    log_started(svc, pid)
	@pids[svc[:name]] = pid
  end

  def log_started(svc, pid)
    puts "Started #{svc[:name]} with command '#{svc[:cmd]}' and pid #{pid}"
    puts svc[:msg]
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
	env
  end

  def make_test_dirs
    base = File.join(ENV['HOME'], "tmp")
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

end

# TODO: Add command line args to specify whether to run unit tests
# or integration tests. For now, we're only running unit tests.
if __FILE__ == $0
  t = TestRunner.new
  t.run_unit_tests
  at_exit { t.stop_all_services }
end
