#!/usr/bin/env ruby

# The Build class provides methods for building Go source files
# for application in the apps directory.
class Build

  def initialize()
    @apps_dir = File.expand_path(File.join(__dir__, "..", "apps"))
    @output_dir = File.expand_path(File.join(__dir__, "..", "bin", "go-bin"))
    @sources = [
      "ingest_pre_fetch/ingest_pre_fetch.go",
      "ingest_validator/ingest_validator.go",
      "reingest_manager/reingest_manager.go",
    ]
  end

  def build(source)
    dir_name, file_name = source.split('/')
    exe_name = file_name.sub(/\.go$/, '')
    cmd = "go build -o #{@output_dir}/#{exe_name} #{file_name}"
    source_dir = "#{@apps_dir}/#{dir_name}"
    puts cmd
    pid = Process.spawn(cmd, chdir: source_dir)
    Process.wait pid
    if $?.exitstatus != 0
      raise "Build failed for #{app.name}"
    end
  end

  def build_all()
    Dir.mkdir(@output_dir) unless File.exists?(@output_dir)
    @sources.each do |source|
      build(source)
    end
    puts "Binaries are in #{@output_dir}"
  end

end


if __FILE__ == $0
  build = Build.new()
  build.build_all()
end
