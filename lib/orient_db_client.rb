require_relative './orient_db_client/connection'
require_relative './orient_db_client/version'
require_relative './orient_db_client/rid'

require 'socket'

module OrientDBClient
	def connect(host, options = {})
    options[:port] = options[:port].to_i
    options[:port] = 2424 if options[:port] == 0

		sok = TCPSocket.open(host, options[:port])

    protocol = BinData::Int16be.read(sok)

		Connection.new(sok, options[:protocol] || protocol)
	end
	module_function :connect

  def db(host, database, username, password, options = {})
    conn = options[:connection]
    conn = connect(host, options) unless conn
    conn.open_database(database, user: username, password: password)
  end
  module_function :db
end
