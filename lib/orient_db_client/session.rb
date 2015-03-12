module OrientDBClient
	class Session
		attr_reader :id

		def initialize(id, connection = nil)
			@id = id
			@connection = connection
    end

    def close(kill_connection)
      if @connection.sessions.length == 0 && kill_connection
        @connection.close unless @connection.closed?
        @connection.closed?  # TODO have the return of this method be homogenous
      else
        # puts 'The existing connection object is returned'
        @connection
      end
    end

	end
end