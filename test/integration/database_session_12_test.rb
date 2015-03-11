require File.join File.dirname(__FILE__), '..', 'test_helper'
require 'json'
require 'benchmark'

class TestDatabaseSession < MiniTest::Unit::TestCase
  include ServerConfig
  include ConnectionHelper

  def setup
    @options = SERVER_OPTIONS
    @connection = connect_to_orientdb(SERVER_OPTIONS)
    @session = @connection.open_database(@options["database"], {
        :user => @options["user"],
        :password => @options["password"]
    })
  end

  def teardown
    @session.close if @session
    @connection.close if @connection && ! @connection.closed?
  end

  # Orient's DB_CLOSE kills access to the session, but returns no response
  def test_close # send false here to leave the connection open
    @session.close
    assert @connection.closed?
  end

  # WIP
  # def test_multi_session
  #   # Create a temp db for the second session
  #   database = 'temp--test'
  #   svr = @connection.open_server(user: 'root', password: 'orient')
  #  	begin
    #   svr.create_local_database(database)
    #   assert server.database_exists?(database)

    #    session2 =

    # ensure
  #     svr.delete_database(database) if svr.database_exists?(database)
    # end

#
  #   svr.close

  # end

  def test_user_query
    result = @session.query("SELECT FROM OUser") # where name = 'admin'
    puts JSON.pretty_generate(result)
    assert_equal @session.id, result[:session], 'Session ID returned should be the same as that already stored in this session'

    result[:message_content].tap do |content|
      assert content.length > 1, 'There should be at least one user'

      content[0].tap do |record|
        assert record[:rid].is_a?(OrientDBClient::Rid)
        assert_equal 'OUser', record[:class], "class should be 'OUser', it is #{record[:class]}"
        record[:document].tap do |fields |
          assert_equal 'ACTIVE', fields['status'], 'User Status should be active'
          assert fields['roles'].is_a?(Array), "expected Array, but got #{fields['roles'].class}"
        end
      end
    end
  end

  def test_single_query
    result = {}
    Benchmark.realtime do
      result = @session.command("select from Family where name = 'Johnson'")
      # result = @session.command("create edge Owns from (select from Person where name = 'Farmer') to (select from Car where name = 'Mazda 626')")
    end
    puts "\n" + JSON.pretty_generate(result)
    assert_equal @session.id, result[:session], 'Session ID returned should be the same as that already stored in this session'
  end

  def test_complex_query
    result = @session.query('select *,$distance as distance from Vehicle where [latitude, longitude, $spatial] NEAR [32.83, -97.04, {"maxDistance":100}]')
    puts JSON.pretty_generate(result)
    assert_equal @session.id, result[:session], 'Session ID returned should be the same as that already stored in this session'
  end

  def test_multi_query
    result = @session.query('SELECT FROM V')
    puts JSON.pretty_generate(result)
    assert_equal @session.id, result[:session], 'Session ID returned should be the same as that already stored in this session'

    result = @session.query('SELECT FROM E')
    puts JSON.pretty_generate(result)
    assert_equal @session.id, result[:session], 'Session ID returned should be the same as that already stored in this session'

    result = @session.query('SELECT FROM OUser')
    puts JSON.pretty_generate(result)
    assert_equal @session.id, result[:session], 'Session ID returned should be the same as that already stored in this session'

    # result = @session.query('SELECT FROM Animal')
    # puts JSON.pretty_generate(result)
    # assert_equal @session.id, result[:session], 'Session ID returned should be the same as that already stored in this session'

  end
  
  def test_multi_create12
    skip # haven't touched this yet
    cluster = "Test123"

    ensure_cluster_exists(@session, cluster)
    @session.reload

    cluster_id = @session.get_cluster(cluster)[:id]
    
    record = { :this => "sucks" }
    
    rid = @session.create_record(cluster_id, record)
    rec = @session.load_record(rid)
    
    
  end
  
  def test_create_class # we now create classes through the session
    res = @session.command('create class testclass'); puts res # cluster id of the class is returned
    refute_nil res[:message_content], 'Failed to create class TestClass'
    res = @session.command('drop class testclass'); puts res
    assert_equal 'true', res[:message_content], 'Failed to drop TestClass'
  end
  
  def test_create_and_delete_record12
    skip # haven't touched this yet

    cluster = "OTest"

    ensure_cluster_exists(@session, cluster)
    @session.reload

    cluster_id = @session.get_cluster(cluster)[:id]
    
    record = { :key1 => 'value1'}

    rid = @session.create_record(cluster_id, record)
    created_record = @session.load_record(rid)
    
    assert_equal cluster_id, rid.cluster_id
    assert_equal 0, rid.cluster_position

    refute_nil created_record
    refute_nil created_record[:document]['key1']

    assert_equal record[:key1], created_record[:document]['key1']

    assert @session.delete_record(rid, created_record[:record_version])
    assert_nil @session.load_record(rid)    

    ensure_cluster_does_not_exist(@session, cluster)
  end

  def test_load_record
    skip # haven't touched this yet
    result = @session.load_record("#4:0")

    assert_equal @session.id, result[:session]

    result[:message_content].tap do |record|
      assert_equal 4, record[:cluster_id]
      assert_equal 0, record[:cluster_position]

      record[:document].tap do |doc|
        assert_equal 'admin', doc['name']
        assert_equal 'ACTIVE', doc['status']

        doc['roles'].tap do |roles|
          assert roles.is_a?(Array), "expected Array, but got #{roles.class}"

          assert roles[0].is_a?(OrientDBClient::Rid)
          assert_equal 4, roles[0].cluster_id
          assert_equal 0, roles[0].cluster_position
        end
      end
    end
  end

end
