require_relative '../rid'
require 'bindata'
require 'base64'

module OrientDBClient
  module Deserializers

    class QueryRecord < BinData::Record
      endian :big

      int16 						:delimiter,		:value => 0
      int8 						  :record_type
      int16 						:cluster_id
      int64         		:cluster_position
      int32         		:version
      int32             :property_len
      string            :properties, :read_length => :property_len
    end

    class RidBin < BinData::Record
      endian :big
      int16 						:cluster_id
      int64         		:cluster_position
    end

    class RidBag < BinData::Record
      endian :big
      int8 				      :config
      int32 						:rid_count
    end

    class StringResponse < BinData::Record
      endian      :big
      int32       :response_len
      string      :response, :read_length => :response_len
    end

    class Deserializer7
      @@string_matcher = /^"[^"]*"$/

      def deserialize(socket)
        records = []
        special_content = nil

        result_type = read_byte socket
        case result_type

        when 108 # a collection of records
          col_len = read_integer socket
          puts "records: #{col_len}"
          col_len.times do
            records << read_record(socket)
          end

        when 110
          special_content = 'none'

        when 97
          special_content = read_string socket

        when 114
          records << read_record(socket)
          # puts 'records: 1'
        else
          if result_type == 1 # we have an error
            special_content = read_error_response(socket)
            puts special_content
          end
        end

        special_content ? special_content : records
      end

      private

      def read_record(socket)
        fields = { :document => {}, :structure => {} }
        struct_info = {}
        qr = QueryRecord.new
        record = qr.read(socket)
        if record.cluster_id.do_num_bytes > 0
          fields[:rid] = OrientDBClient::Rid.new("##{record.cluster_id}:#{record.cluster_position}")
        end
        # Parse the document proper
        tokens = record.properties.split(',')
        while token = tokens.shift
          arr = token.split('@', 2) # case of class
          if arr.length == 2
            fields[:class] = arr[0]
            token = arr[1]
          end
          field, value = parse_field(token, tokens, struct_info)
          fields[field] = value if field == :class
          fields[:document][field] = value
          fields[:structure][field] = struct_info[:type]
        end
        fields
      end

      def read_error_response(socket)
        errs = []
        4.times do |i|
          errs << read_string(socket)
          if i == 1
            break if read_byte(socket) != 1
          end
        end
        errs.join("\n")
      end

      def read_byte(socket)
        BinData::Int8.read(socket).to_i
      end

      def read_integer(socket)
        BinData::Int32be.read(socket).to_i
      end

      def read_long(socket)
        BinData::Int64be.read(socket).to_i
      end

      def read_short(socket)
        BinData::Int16be.read(socket).to_i
      end

      def read_string(socket)
        bin_length = read_integer(socket)
        return nil if bin_length < 0

        raise bin_length.inspect if bin_length < 0

        bin_str = socket.read(bin_length)
        bin_str.length > 0 ? bin_str : nil
      end

      def unpack_hex_bytes(slice)
        slice.unpack("H*").first.to_i(16)
      end

      # If the token was split too short on ','
      def close_token!(token, cap, join, tokens)
        while token[token.length - 1] != cap && tokens.length > 0
          token << join if join
          token << tokens.shift
        end
        token
      end

      def remove_ends(token)
        token[1...token.length-1]
      end

      def parse_date(value)
        time = parse_time(value)

        Date.new(time.year, time.month, time.day)
      end

      def parse_field(token, tokens, struct_info)
        field, value = token.split(':', 2)
        field = remove_ends(field) if field.match(@@string_matcher)
        if (field =~ /^(in|out)_/) != nil
          value =  parse_rid_bag(value, struct_info)
        else
          value = parse_value(value, tokens, struct_info)
        end
        return field, value
      end

      def parse_rid_bag(value, struct_info)
        value = Base64.decode64(value)
        return '' if value.length < 1
        rb = RidBag.new.read value

        return [] if rb.rid_count < 1
        struct_info[:type] = :collection
        value = value[5..-1]
        rid_bin = RidBin.new
        rids = []
        rb.rid_count.times do
          r = rid_bin.read(value)
          rids << OrientDBClient::Rid.new("##{r.cluster_id}:#{r.cluster_position}")
          value = value[10..-1]
        end
        rids
      end

      def parse_time(value)
        Time.at(value[0...value.length - 1].to_i).utc
      end

      def parse_value(value, tokens, struct_info = {})
        struct_info[:type] = nil
        return nil unless value

        case value[0]
          when '['
            close_token!(value, ']', ',', tokens)
            sub_tokens = remove_ends(value).split(',')
            value = []

            while element = sub_tokens.shift
              value << parse_value(element, sub_tokens)
            end
            struct_info[:type] = :collection
            value

          when '<'
            close_token!(value, '>', ',', tokens)
            sub_tokens = remove_ends(value).split(',')
            value = []

            while element = sub_tokens.shift
              value << parse_value(element, sub_tokens)
            end
            struct_info[:type] = :collection
            value

          when '{'
            close_token!(value, '}', ',', tokens)
            struct_info[:type] = :map
            deserialize(remove_ends(value))[:document]

          when '('
            close_token!(value, ')', ',', tokens)
            struct_info[:type] = :document
            deserialize remove_ends(value)

          when '*'
            close_token!(value, '*', ',', tokens)
            struct_info[:type] = :document
            deserialize remove_ends(value)

          when '"'
            close_token!(value, '"', ',', tokens)
            struct_info[:type] = :string
            value.gsub! /^\"/, ''
            value.gsub! /\"$/, ''
          when '_'
            close_token!(value, '_', ',', tokens)
            struct_info[:type] = :binary
            Base64.decode64(remove_ends(value))
          when '#'
            struct_info[:type] = :rid
            OrientDBClient::Rid.new(value)
          else
            if value.length == 0
              nil
            elsif value == 'null'
              nil
            elsif value == 'true'
              struct_info[:type] = :boolean
              true
            elsif value == 'false'
              struct_info[:type] = :boolean
              false
            else
              case value[value.length - 1]
                when 'b'
                  struct_info[:type] = :byte
                  value.to_i
                when 's'
                  struct_info[:type] = :short
                  value.to_i
                when 'l'
                  struct_info[:type] = :long
                  value.to_i
                when 'f'
                  struct_info[:type] = :float
                  value.to_f
                when 'd'
                  struct_info[:type] = :double
                  value.to_f
                when 'c'
                  struct_info[:type] = :decimal
                  value.to_f
                when 't'
                  struct_info[:type] = :time
                  parse_time(value)
                when 'a'
                  struct_info[:type] = :date
                  parse_date(value)
                else
                  struct_info[:type] = :integer
                  value.to_i
              end
            end
        end
      end
    end
  end
end