#!/usr/bin/env ruby

#Bring OptionParser into the namespace
require "rubygems"
require "open4" #it captures the standard output and error stream
require 'optparse'
require 'English' #It allow us to access variable $? via the more memorable $CHILD_STATUS
require 'yaml'

options = {
  :gzip => true,
  :force => false,
  :'end-of-iteration' => false,
  :username => nil,
  :password => nil,
}

CONFIG_FILE = File.join(ENV['HOME'],'.db_backup.rc.yaml')
if File.exists? CONFIG_FILE
  config_options = YAML.load_file(CONFIG_FILE)
  options.merge!(config_options)
else
  File.open(CONFIG_FILE,"w") {|file| YAML::dump(options,file)}
  STDERR.puts "Initialized configuration file in #{CONFIG_FILE}"
end


option_parser = OptionParser.new do |opts|
  executable_name = File.basename($PROGRAM_NAME)
  opts.banner = "Backup one or more MYSQL databases

  Usage: #{executable_name} [options] database_name"
  opts.on("-i","--end-of-iteration", 'Indicate that this backup is an "iteration" backup') do
    options[:iteration] = true
  end

  opts.on("-u USER","--username",/^.+$/,'Database username') do |user|
    options[:username] = user
  end

  opts.on("-p PASSWORD","--pasword",'Database Password') do |password|
    options[:password] = password
  end

  opts.on("--no-gzip","Do not compress the backup file") do |a|
    options[:gzip] = false
  end

  opts.on("--[no-]force","Overwrite existing files") do |force|
    options[:force] = force
  end
end

exit_status = 0
begin
  option_parser.parse!
  if ARGV.empty?
    puts "\nerror: you must supply a database name"
    puts
    puts "#{option_parser.help}"
    exit_status |= 0b0010
  else
    database_name = ARGV[0]
  end
rescue OptionParser::InvalidArgument => ex
  puts ex.message
  puts option_parser
  exit_status |= 0b0001
end

exit exit_status unless exit_status == 0

# Proceed with the rest of the program

auth = ""
auth += "-u#{options[:username]} " if options[:username]
auth += "-p#{options[:password]} " if options[:password]

if options[:iteration]
  backup_file_name = database_name + '_' + ARGV[1]
else
  backup_file_name = database_name + '_' + Time.now.strftime('%Y%m%d')
end

output_file = "db_backup/#{backup_file_name}.sql"

if File.exists? output_file
  if options[:force]
    STDERR.puts "Overwriting #{output_file}"
  else
    STDERR.puts "error #{output_file} exists, use --force to overwrite"
    exit 1
  end
end

command = "mysqldump #{auth}#{database_name} > #{output_file}"
system(command)
puts "Running '#{command}'"

pid, stdin, stdout, stderr = Open4::popen4(command)
_, status = Process::waitpid2(pid)

unless status.exitstatus == 0
  puts "There was a problem running '#{command}'"
#  puts stderr
  exit -1
end


if options[:gzip]
  `gzip #{output_file}`
end

#Catch Signal recieved from Ctrl + C and it clean ups database dump if
#it is already created
Signal.trap("SIGINT") do
  FileUtils.rm output_file
  exit 1
end
