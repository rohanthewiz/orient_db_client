require_relative './session'

module OrientDBClient
	class DatabaseSession < Session
    STATUS_OPENED = 'opened'.freeze
    STATUS_CLOSED = 'closed'.freeze
		attr_reader :database, :server_release, :clusters
    attr_accessor :status

		def initialize(database, id, connection, clusters = [], server_release = '')
			super id, connection
      @database = database
      @server_release = server_release
      @status = STATUS_OPENED
      store_clusters(clusters)
		end

		def close(kill_connection = true)
      @connection.close_db_session(@id) if @status == STATUS_OPENED # close this session
      @status = STATUS_CLOSED
      super
		end

		def cluster_exists?(cluster_id)
			@connection.cluster_exists?(@id, cluster_id)
		end

		def count(cluster_name)
			@connection.count(@id, cluster_name)
		end

		def create_physical_cluster(name, options = {})
			options.merge!({ :name => name })

			@connection.create_cluster(@id, :physical, options)
		end

		def clusters
			@clusters.values
		end

		def create_record(cluster_id, record)
			@connection.create_record(@id, cluster_id, record)
		end

		def delete_cluster(cluster_id)
			@connection.delete_cluster(@id, cluster_id)
		end

		def delete_record(rid_or_cluster_id, cluster_position_or_version, version = nil)
			if rid_or_cluster_id.is_a?(OrientDBClient::Rid)
				rid = rid_or_cluster_id
				version = cluster_position_or_version.to_i
			else
				rid = OrientDBClient::Rid.new(rid_or_cluster_id.to_i, cluster_position_or_version.to_i)
				version = version
			end
			
			@connection.delete_record(@id, rid, version)
		end

		def get_cluster(id)
			if id.kind_of?(Fixnum)
				@clusters[id]
			else
				@clusters_by_name[id.downcase]
			end
		end

		def get_cluster_datarange(cluster_id)
			@connection.get_cluster_datarange(@id, cluster_id)
		end

		def load_record(rid_or_cluster_id, cluster_position = nil)
			if rid_or_cluster_id.is_a?(Fixnum)
				rid_or_cluster_id = OrientDBClient::Rid.new(rid_or_cluster_id, cluster_position)
			end
			
			@connection.load_record(@id, rid_or_cluster_id)[:message_content]
		end

		def query(text, options = {})
			@connection.query(@id, text, options)
		end

		def command(text, options = {})
			@connection.command(@id, text, options)
		end

		def reload
			@connection.reload(@id)
		end

		def update_record(record, rid_or_cluster_id, cluster_position_or_version, version = :none)
			if rid_or_cluster_id.is_a?(Fixnum)
				rid = OrientDBClient::Rid.new(rid_or_cluster_id, cluster_position_or_version)
				version = version
			else
				rid = rid_or_cluster_id
				version = cluster_position_or_version
			end
			
			@connection.update_record(@id, rid, record, version)
		end

		private

		def store_clusters(clusters)
			@clusters = {}
			@clusters_by_name = {}

			clusters.each do |cluster|
				@clusters[cluster[:id]] = cluster
				@clusters_by_name[cluster[:name].downcase] = cluster
			end
		end
	end
end