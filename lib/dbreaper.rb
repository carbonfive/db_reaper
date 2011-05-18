require 'active_record'
class DbReaperError < StandardError; end

module DbReaper
  DEFAULT_CONFIG = { :reaper_data_dir => 'reaped',
    :move_records => true,
    :expiry => 7776000,
    :dump_to_file => true,
    :preserve_backup_table => false,
    :backup_table_prefix => 'reaper_',
    :logger => (defined? RAILS_DEFAULT_LOGGER) ? RAILS_DEFAULT_LOGGER : Logger.new(STDERR)
  }

  class Config 
    attr_accessor :reaper_data_dir, :backup_table_prefix, :logger, :move_records, :expiry, 
                  :dump_to_file, :preserve_backup_table
    def initialize(args = {})
      self.merge(DEFAULT_CONFIG)
      args.each do |k,v|
        self.send(k.to_s+"=",v) unless v.nil?
      end
    end

    def merge opts
      opts.keys.each do |k|
        assign = k.to_s + "="
        if (self.respond_to? assign) && (!opts[k].nil?)
          self.send(assign, opts[k])
        end
      end
      self
    end

    def to_s
      config.attributes.join(', ')
    end
  end

  delegate :logger, :to => :config

  def self.config
    @@config ||= Config.new()
  end
  
  def self.configure
    yield self.config
  end

######
#  To setup an activerecord class to mix this in, you should include it as follows
#
#  class MyDbClass < ActiveRecord::Base
#
#     extend DbReaper
#
#  configuration for reaper on a per class basis should be in reaper.yml
#  and add an initializer to load that config data

  
  def reap options = {}
    unless has_mysqldump
      self.logger.error("mysqldump is not available in your environment.  DbReaper will not do anything until this has been resolved.")
      return 0
    end
    # each time we reap, set the start time
    reap_time = Time.now.strftime('%Y%m%d%H%M%S')

    # most options are managed in the yaml config file
    # options that can be passed in here are the typical :conditions, :limit, :order the same as you 
    # would pass to sql finder.
    # By default, a 'created_at < expiry' clause is added where expiry is taken from the reaper yaml config
    # and can be individually specified for each class for which DbReaper is mixed in.
    # To disable that created_at clause, add 'ignore_expiry' => true to the input options.
    # If this is in place, :conditions will define what is cleaned an what is not
    backup_table_name = "#{config.backup_table_prefix}#{table_name}_#{reap_time}"

    # update conditions
    conditions = [options.delete(:conditions)]
    conditions << ['created_at < ?', config.expiry.seconds.ago] unless options[:ignore_expiry]
    
    options[:conditions] = conditions.reject{|opt| opt.blank?}.map{|c| "(#{sanitize_sql(c)})"}.join(' and ')

    rows_reaped = copy_sql_table backup_table_name, options

    if config.dump_to_file
      dump_backup_table backup_table_name, reap_time
    end
    rows_reaped
  end

  private
  def dump_backup_table backup_table_name, reap_time
    dbcfg = ActiveRecord::Base.connection.instance_variable_get(:@config)
    dbcfg['username'] ||= 'root'
    
    # build outfile name based on timestamp 
    fname = "#{dbcfg['database']}.reaped_#{table_name}_#{reap_time}.sql"
    dir = File.join(config.reaper_data_dir, table_name)
    FileUtils.mkdir_p(dir, :mode => 0775)
    reaper_output_file = File.join(dir, fname)

    # build commandline as array
    args = [ mysqldump ]
    args << "--socket=#{dbcfg['socket']}" if dbcfg.has_key? 'socket'
    args << "--port=#{dbcfg['port']}" if dbcfg.has_key? 'port'
    args << "--host=#{dbcfg['host']}" if dbcfg.has_key? 'host'
    args << "--user=#{dbcfg['username']}" if dbcfg.has_key? 'username'
    args << "--password=#{dbcfg['password']}" if dbcfg.has_key? 'password'
    args << "#{dbcfg['database']} #{backup_table_name}"
    args << " > #{reaper_output_file}"
    cmd = args.join(" ")
    success = system cmd
    if success == true
      logger.debug("Reaped data to #{reaper_output_file}")
      # on success, we drop the temporary table
      ActiveRecord::Base.connection.execute("drop table if exists `#{backup_table_name}`") unless config.preserve_backup_table # default should be false
    else
      msg = "Failed to run mysqldump cmd [#{cmd}]. Error from system #{$?}. Data is left in the database under #{backup_table_name}"
      self.logger.error(msg)
      raise DbReaperError.new(msg)
    end
    success
  end

  def copy_sql_table backup_table_name, opts
    dbconn = ActiveRecord::Base.connection
    begin
      dbconn.begin_db_transaction
      # leveraging private ActiveRecord methods
      sql = construct_copy_sql backup_table_name, opts
      dbconn.execute("drop table if exists `#{backup_table_name}`")
      dbconn.execute(sql)
      if config.move_records
        delete_sql = construct_delete_sql backup_table_name, opts
        dbconn.execute(delete_sql)
      end
      rows_reaped = dbconn.execute("select count(*) from `#{backup_table_name}`").fetch_row().first.to_i
      dbconn.commit_db_transaction
    rescue Exception => ex
      dbconn.rollback_db_transaction
      raise ex
    end
    rows_reaped
  end

  def construct_delete_sql backup_table_name, opts
    (construct_finder_sql opts).gsub(/^select \*/i, 'delete')
  end

  def construct_copy_sql backup_table_name, opts = {}
    raise DbReaperError.new("backup_table_name cannot be empty") if (!backup_table_name || backup_table_name.blank?)
    sql = construct_finder_sql opts
    "create table `#{backup_table_name}` " + sql
  end

  def mysqldump 
    path_to_mysql_dump
  end

  def has_mysqldump 
    system "which mysqldump > /dev/null"
  end
  
  def path_to_mysql_dump
    `which mysqldump`.strip
  end

end

