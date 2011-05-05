require File.dirname(__FILE__) + '/../spec_helper'
require 'active_record'
require 'dbreaper'

class MyReapable < ActiveRecord::Base
  extend DbReaper
end

describe DbReaper do
  before do
    ActiveRecord::Migration.create_table :my_reapables, :force => true do |t|
      t.timestamps
      t.string :name
    end
    5.times.each do |t|
      MyReapable.create(:created_at => (t+2).weeks.ago)
    end
    @reaped_table_name = "reaped_my_reapables_#{Time.now.strftime('%Y%m%d%H%M%S')}"
    MyReapable.stubs(:config => valid_config,
              :system => true)
  end
  describe "#reap" do
    it "class responds to the reap method (mixin works)" do
      MyReapable.respond_to?(:reap).should be_true
    end
    it 'starts a db transaction' do
      MyReapable.reap 
    end
    context "when an exception is raised (e.g. call #reap with an empty filename)" do
      it "errors are logged" do
        MyReapable.stubs(:system => false)
        Logger.any_instance.expects(:error)
        begin 
          MyReapable.reap
        rescue
        end
      end
    end
    context "when mysqldump is not available" do
      before do 
        MyReapable.expects(:system).with('which mysqldump > /dev/null').at_least_once.returns(false)
      end
      it "logs an error" do
        Logger.any_instance.expects(:error)
        MyReapable.reap
      end
      it "does not call other methods" do
        MyReapable.expects(:copy_sql_table).never
        MyReapable.expects(:dump_backup_table).never
        MyReapable.reap
      end
    end
    
    context "given a bad query (e.g. conditions with columns that don't exist on the model)" do
      it "rolls back the db transaction" do
        ActiveRecord::Base.connection.expects(:rollback_db_transaction).once
        lambda{ MyReapable.reap :conditions => 'nonexistent_column = 1' }.should raise_error ActiveRecord::StatementInvalid
      end
      it 'does not commit the transaction' do
        ActiveRecord::Base.connection.expects(:commit_db_transaction).never
        lambda{ MyReapable.reap :conditions => 'nonexistent_column = 1' }.should raise_error ActiveRecord::StatementInvalid
      end
    end
    context "with valid parameters" do
      it 'returns the number of records reaped' do
        MyReapable.reap.should == 2
      end
      it "the old table maintains the records if the configuration says move_records is false" do
        MyReapable.expects(:config).at_least_once.returns(valid_config({"move_records" => false}))
        expect{ MyReapable.reap }.to change(MyReapable, :count).by(0)
        execute_and_fetch("select id from `#{@reaped_table_name}`").flatten.should == ["4", "5"].sort
      end
      it "the old table no longer contains the reaped records and maintains :conditions and expiry" do
        cfg = valid_config
        expiry = cfg["expiry"]
        partitioned = MyReapable.all.partition{|lg| (lg.id % 2) == 0 && lg.created_at < expiry.seconds.ago}
        MyReapable.reap  :conditions => 'mod(id,2) = 0'
        execute_and_fetch("select id from `#{@reaped_table_name}`").flatten.map(&:to_i).should == partitioned[0].map(&:id)
        execute_and_fetch("select id from `my_reapables`").flatten.map(&:to_i).should == partitioned[1].map(&:id)
        MyReapable.all.select{|lg| (lg.id % 2) == 0 && lg.created_at < expiry.seconds.ago}.should have(0).entries
        MyReapable.all.select{|lg| (lg.id % 2) == 0 }.should have(1).entries
      end
      it "the old table no longer contains the reaped records and uses only :conditions given 'ignore_expiry' => true" do
        cfg = valid_config
        partitioned = MyReapable.all.partition{|lg| (lg.id % 2) == 0}
        MyReapable.reap  :conditions => 'mod(id,2) = 0', :ignore_expiry => true
        execute_and_fetch("select id from `#{@reaped_table_name}`").flatten.map(&:to_i).should == partitioned[0].map(&:id)
        execute_and_fetch("select id from `my_reapables`").flatten.map(&:to_i).should == partitioned[1].map(&:id)
        MyReapable.all.select{|lg| (lg.id % 2) == 0 }.should have(0).entries
      end
    end
    after do
      cleanup
    end
  end
  describe "#dump_backup_table" do
    before do
      FileUtils.stubs(:mkdir_p)
    end
    it "returns true on success" do
      MyReapable.send(:dump_backup_table,'ablename','timestamp').should == true
    end
    it "raises DbReaperError on failure" do
      MyReapable.stubs(:system => false)
      lambda{ MyReapable.send(:dump_backup_table,'tablename','timestamp') }.should raise_error(DbReaperError)
    end
  end
  after do
    ActiveRecord::Migration.drop_table :my_reapables
  end
end


### helpers for tests
def execute_and_fetch sql
  result = []
  ActiveRecord::Base.connection.execute(sql).each do |h|
    result << h
  end
  result
end

def valid_config(cfg = {}) 
  table_cfg = DbReaper::ReapableConfig.new(:move_records =>  true,
                                           :expiry =>  2592000,
                                           :dump_to_file =>  true,
                                           :preserve_backup_table => true)
  DbReaper::Config.new(:reaper_data_dir => 'reapit_test',
                       :default => table_cfg)
end

def reaper_output_file
  File.join('/tmp', 'reapit_test','logs',"myreapable_test." + @reaped_table_name + ".sql")
end

def cleanup
  ActiveRecord::Base.connection.execute("drop table if exists `#{@reaped_table_name}`")
  dir = File.join(valid_config['reaper_data_dir'], 'logs')
  Dir.glob(File.join(dir,'*.sql')).each do |f|
    FileUtils.rm(f)
  end
end
