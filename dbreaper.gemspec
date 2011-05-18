# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "dbreaper/version"

Gem::Specification.new do |s|
  s.name        = "dbreaper"
  s.version     = Dbreaper::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Mr Rogers", "Jeremy Yun"]
  s.email       = ["jon@bunnymatic.com"]
  s.homepage    = "http://github.com/bunnymatic"
  s.summary     = %q{DbReaper}
  s.description = %q{}

  s.rubyforge_project = "dbreaper"
  
  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  
  s.add_dependency 'activerecord', "~> 2.3"

  s.add_development_dependency "rspec", "~> 1.3"
  s.add_development_dependency "mocha"
  # found issues using mysql 2.8 and running spec
  s.add_development_dependency "mysql", "2.7"
end

