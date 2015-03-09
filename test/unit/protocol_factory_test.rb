require File.join File.dirname(__FILE__), '..', 'test_helper'

require 'orient_db_client/protocol_factory'

class TestProtocolFactory < MiniTest::Unit::TestCase
	def test_returns_protocol7_instance
 	 	assert_equal OrientDBClient::Protocols::Protocol7, OrientDBClient::ProtocolFactory.get_protocol(7)
 	end

    def test_returns_protocol9_instance
        assert_equal OrientDBClient::Protocols::Protocol9, OrientDBClient::ProtocolFactory.get_protocol(9)
    end
end

