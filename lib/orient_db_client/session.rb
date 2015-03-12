module OrientDBClient
	class Session
		attr_reader :id

		def initialize(id, connection = nil)
			@id = id
			@connection = connection
    end

    # Returns nil success, or connection object if @connection is still open
    def close(kill_connection)
      if @connection.sessions.length == 0 && kill_connection
        @connection.close unless @connection.closed?
        return @connection.closed? ? nil : @connection
      else
        # puts 'The existing connection object is returned'
        @connection
      end
    end

	end
end