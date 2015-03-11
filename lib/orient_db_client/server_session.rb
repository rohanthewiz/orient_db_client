require_relative './session'

module OrientDBClient
	class ServerSession < Session

    def close(kill_connection = true)
      @connection.remove_session @id
      super
    end

    def create_local_database(database, options = {})
			options[:storage_type] = :plocal

			@connection.create_database(@id, database, options)
		end

		def create_memory_database(database, options = {})
			options[:storage_type] = :memory

			@connection.create_database(@id, database, options)
		end
		
		def config_get(config_name)
		  @connection.config_get(@id, config_name)
		end

		def database_exists?(database)
			@connection.database_exists?(@id, database)
		end

		def delete_database(database)
			@connection.delete_database(@id, database)
		end
	end
end