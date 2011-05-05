ENV["RAILS_ENV"] ||= 'test'
require 'active_record'
Spec::Runner.configure do |config|
  ActiveRecord::Base.establish_connection(:host=>'localhost',
                                          :adapter=>'mysql',
                                          :username => 'root', 
                                          :database => 'test')
  config.mock_with :mocha

end
