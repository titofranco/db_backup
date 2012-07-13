#!/usr/bin/env ruby
require "rubygems"
require "open4"  #it captures the standard output and error stream
require "optparse" #Bring OptionParser into the namespace
require 'English' #It allow us to access variable $? via the more memorable $CHILD_STATUS
require 'yaml'

options = {
  :username => nil,
  :password => nil,
  :path_to_file => nil,
  :path_to_app => "~/projects/inventory"
}

CONFIG_FILE = File.join(ENV['HOME'],'.data_import.rc.yaml')
if File.exists? CONFIG_FILE
  config_options = YAML.load_file(CONFIG_FILE)
  options.merge!(config_options)
else
  File.open(CONFIG_FILE,"w") {|file| YAML::dump(options,file)}
  STDERR.puts "Initialized configuration file in #{CONFIG_FILE}"
end

option_parser = OptionParser.new do |opts|
  executable_name = File.basename($PROGRAM_NAME)
  opts.banner = "Make an import of a csv file to a mysql table of the inventory app
  \n\nUsage: #{executable_name} [options] database_name"

  opts.on("-a","--all","Import all files") do
    options[:all] = true
  end

  opts.on("--drop-create","Drop and create the specified table given by --filename option") do
    options[:dropcreate] = true
  end

  opts.on("-e RAILS_ENV","--rails-env","Use this option only if need to drop and create a specific table") do |rails_env|
    options[:railsenv] = rails_env
  end

  opts.on("-f FILENAME","--filename","Import a specific file. Should be in plural") do |filename|
    options[:filename] = filename
  end

  opts.on("-u USER","--username",'Database username') do |user|
    options[:username] = user
  end

  opts.on("-p PASSWORD","--password",'Database password') do |pwd|
    options[:password] = pwd
  end
end

exit_status =  0

begin
  option_parser.parse!
  if ARGV.empty?
    puts "\nerror: you must supply a database name\n"
    puts "#{option_parser.help}"
    exit_status |= 0b0010
  elsif options[:filename].nil? && options[:all].nil?
    puts "\nerror: you must supply a file name\n"
    puts "#{option_parser.help}"
    exit_status |= 0b0100
  elsif options[:dropcreate] && options[:railsenv].nil?
    puts "\nerror: you must supply the rails enviroment\n"
    puts "#{option_parser.help}"
    exit_status |= 0b0101
  else
    database_name = ARGV[0]
  end
rescue OptionParser::InvalidArgument => ex
  puts ex.message
  puts option_parser
  exit_status |= 0b0001
end

exit exit_status unless exit_status == 0

auth = ""
auth += "-u#{options[:username]} " if options[:username]
auth += "-p#{options[:password]} " if options[:password]
file_to_import = options[:all] ? "all" : options[:filename]

@rails_env = options[:railsenv]
drop_create = options[:dropcreate]
#path_to_file = File.expand_path("../../lib/text_files")
path_to_file = options[:path_to_file]
@path_to_app = options[:path_to_app]

def migration_command(version_number)
  "cd #{@path_to_app} && bundle exec rake db:migrate:down VERSION=#{version_number} RAILS_ENV=#{@rails_env} ; bundle exec rake db:migrate:up VERSION=#{version_number} RAILS_ENV=#{@rails_env};"
end

dc_states = migration_command(20110722140646)
states =  " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1  --columns=id,name,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/states.csv; "

dc_cities = migration_command(20110722140939)
cities =  " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1  --columns=id,state_id,name,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/cities.csv; "

dc_categories = migration_command(20110718223500)
categories = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1  --columns=id,name,description,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/categories.csv; "

dc_invoice_enumerations = migration_command(20110921145812)
invoice_enumerations =  " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=invoice_type,issue_date,prefix,initial_number,final_number,resolution_number,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/invoice_enumerations.csv; "

dc_products = "cd #{@path_to_app} && bundle exec rake db:migrate:down VERSION=20110718200308 RAILS_ENV=#{@rails_env} ; cd #{@path_to_app} && bundle exec rake db:migrate:down VERSION=20110718192227 RAILS_ENV=#{@rails_env} ; cd #{@path_to_app} && bundle exec rake db:migrate:up VERSION=20110718192227 RAILS_ENV=#{@rails_env} ; cd #{@path_to_app} && bundle exec rake db:migrate:up VERSION=20110718200308  RAILS_ENV=#{@rails_env};"
products = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,barcode,category_id,name,origin,manufacturer,reference,location,quantity,avg_cost,price,status,vat_pct,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/products.csv; "

dc_supplier_product_listings = migration_command(20120509165420)
supplier_product_listings = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=category_name,car_make,reference,alternate_reference,description,manufacturer,origin,price #{auth} --local #{database_name} #{path_to_file}/supplier_product_listings.csv;"

dc_car_makes = migration_command(20120511200801)
car_makes = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1  --columns=id,name,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/car_makes.csv; "

dc_sales = migration_command(20110915145625)
sales = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,type,customer_id,entry_date,expiration_date,invoice_number,gross_value,disc_pct,discount,vat,total,paid_value,refund_value,balance,status,note,user_id,created_at,updated_at  #{auth} --local #{database_name} #{path_to_file}/sales.csv;"

dc_sale_details = migration_command(20110915154446)
sale_details = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=sale_id,product_id,quantity,suggested_price,sale_price,vat_pct,total,status,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/sale_details.csv;"

dc_purchases = migration_command(20110726144725)
purchases = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,type,entry_date,expiration_date,supplier_id,invoice_number,gross_value,disc_pct,discount,freight,vat,withholding_source,total,paid_value,refund_value,balance,note,consecutive_number,status,user_id,created_at,updated_at  #{auth} --local #{database_name} #{path_to_file}/purchases.csv; "

dc_purchase_details = migration_command(20110728223349)
purchase_details = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=purchase_id,product_id,quantity,cost,vat_pct,total,status,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/purchase_details.csv;"

dc_cash_receipts = migration_command(20110930153600)
cash_receipts  = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,receipt_number,issue_date,customer_id,gross_value,discount,freight,vat,subtotal,withholding_vat,withholding_source,withholding_ica,refund_value,paid_value,balance,total,amount_received,total_paid,apply_commission,employee_id,user_id,note,status,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/cash_receipts.csv; "

dc_cash_receipt_details = migration_command(20110930155108)
cash_receipt_details  = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=cash_receipt_id,sale_id,total,paid_value,refund_value,balance,amount_paid,status  #{auth} --local #{database_name} #{path_to_file}/cash_receipt_details.csv; "

dc_payment_receipts = migration_command(20110811193711)
payment_receipts = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,type,entry_date,receipt_number,supplier_id,description,gross_value,discount,freight,vat,withholding_source,subtotal,additional_ws,additional_discount,refund_value,paid_value,balance,total,amount_paid,total_paid,user_id,note,status,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/payment_receipts.csv; "

dc_payment_receipt_details = migration_command(20110812193840)
payment_receipt_details = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=payment_receipt_id,purchase_id,total,paid_value,refund_value,balance,amount_paid,status #{auth} --local #{database_name} #{path_to_file}/payment_receipt_details.csv; "

dc_payment_methods = migration_command(20111103211734)
payment_methods = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,feature_id,feature_type,entry_date,payment_type,check_number,bank_number,reference_number,amount,note,status,created_at,updated_at  #{auth} --local #{database_name} #{path_to_file}/payment_methods.csv; "

dc_suppliers = migration_command(20110721210926)
suppliers = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,name,id_type,id_number,regime_type,large_taxpayer,large_withholder,bank_name,account_type,account_number,agreement_number,main_number,office_number,fax,cellphone,contact_person,address,city_id,state_id,email,web_page,note,status,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/suppliers.csv; "

dc_customers = migration_command(20110914183651)
customers = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,id_type,id_number,name,main_number,fax,cellphone,email,address,state_id,city_id,note,status,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/customers.csv; "

dc_employees = migration_command(20111004160111)
employees = "  mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,id_type,id_number,first_name,last_name,birth_date,sex,cellphone,home_phone,other_phone,email,address,city_id,state_id,hire_date,fired_date,position,salary,commission_pct,note,status,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/employees.csv; "

dc_withholdings = migration_command(20110818160607)
withholdings = "  mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,year,limit_value,percentage,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/withholdings.csv; "

dc_purchase_refunds = migration_command(20111123150756)
purchase_refunds = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,invoice_number,purchase_id,supplier_id,issue_date,entry_date,gross_value,disc_pct,discount,vat,total,user_id,status,note,created_at,updated_at  #{auth} --local #{database_name} #{path_to_file}/purchase_refunds.csv;"

dc_purchase_refund_details = migration_command(20111123151149)
purchase_refund_details = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,purchase_refund_id,product_id,quantity,cost,total,status,created_at,updated_at #{auth} --local #{database_name} #{path_to_file}/purchase_refund_details.csv;"


dc_sale_refunds = migration_command(20111123165012)
sale_refunds = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,invoice_number,sale_id,customer_id,issue_date,entry_date,gross_value,disc_pct,discount,vat,total,user_id,status,note,created_at,updated_at  #{auth} --local #{database_name} #{path_to_file}/sale_refunds.csv;"

dc_sale_refund_details = migration_command(20111123165830)
sale_refund_details = " mysqlimport --delete --fields-terminated-by='|' --ignore-lines=1 --columns=id,sale_refund_id,product_id,quantity,sale_price,total,status,created_at,updated_at  #{auth} --local #{database_name} #{path_to_file}/sale_refund_details.csv;"


command = ""
case file_to_import
when 'states' then
  command = drop_create ? dc_states + states : states
when 'cities' then
  command = drop_create ? dc_cities + cities : cities
when 'categories' then
  command = drop_create ? dc_categories + categories : categories
when 'invoice_enumerations' then
  command = drop_create ? dc_invoice_enumerations + invoice_enumerations : invoice_enumerations
when 'products' then
  command = drop_create ? dc_products + products : products
when 'supplier_product_listings' then
  command = drop_create ? dc_supplier_product_listings + supplier_product_listings : supplier_product_listings
when 'car_makes' then
  command = drop_create ? dc_car_makes + car_makes : car_makes
when 'sales' then
  command = drop_create ? dc_sales + sales : sales
when 'sale_details' then
  command = drop_create ? dc_sale_details + sale_details : sale_details
when 'purchases' then
  command = drop_create ? dc_purchases + purchases : purchases
when 'purchase_details' then
  command = drop_create ? dc_purchase_details + purchase_details : purchase_details
when 'cash_receipts' then
  command = drop_create ? dc_cash_receipts + cash_receipts : cash_receipts
when 'cash_receipt_details' then
  command = drop_create ? dc_cash_receipt_details + cash_receipt_details : cash_receipt_details
when 'payment_receipts' then
  command = drop_create ? dc_payment_receipts + payment_receipts : payment_receipts
when 'payment_receipt_details' then
  command = drop_create ? dc_payment_receipt_details + payment_receipt_details : payment_receipt_details
when 'payment_methods' then
  command = drop_create ? dc_payment_methods + payment_methods : payment_methods
when 'suppliers' then
  command = drop_create ? dc_suppliers + suppliers : suppliers
when 'customers' then
  command = drop_create ? dc_customers + customers : customers
when 'employees' then
  command = drop_create ? dc_employees + employees : employees
when 'withholdings' then
  command = drop_create ? dc_withholdings + withholdings : withholdings
when 'purchase_refunds' then
  command = drop_create ? dc_purchase_refunds + purchase_refunds : purchase_refunds
when 'purchase_refund_details' then
  command = drop_create ? dc_purchase_refund_details + purchase_refund_details : purchase_refund_details
when 'sale_refunds' then
  command = drop_create ? dc_sale_refunds + sale_refunds : sale_refunds
when 'sale_refund_details' then
  command = drop_create ? dc_sale_refund_details + sale_refund_details : sale_refund_details


when 'all' then
  command = states + cities + categories + invoice_enumerations + products + sales + sale_details + purchases + purchase_details + cash_receipts + cash_receipt_details + payment_receipts + payment_receipt_details + payment_methods + suppliers + customers + employees + withholdings + purchase_refunds + purchase_refund_details + sale_refunds + sale_refund_details + supplier_product_listings + car_makes
end

system(command)
#puts "Running '#{command}'"

pid, stdin, stdout, stderr = Open4::popen4(command)
_, status = Process::waitpid2(pid)

unless status.exitstatus == 0
  puts "There was a problem running '#{command}'"
  exit -1
end
