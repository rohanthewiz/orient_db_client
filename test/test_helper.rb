require 'rubygems'
gem 'minitest' # ensures you're using the gem, and not the built in MT
require 'minitest/autorun'
require 'mocha'

Dir[File.expand_path(File.join(File.dirname(__FILE__),'support','**','*.rb'))].each {|f| require f}

require_relative '../lib/orient_db_client'
require_relative '../lib/orient_db_client/exceptions'
require 'pry-debugger'
