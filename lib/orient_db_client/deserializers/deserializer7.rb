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
      # string						:rids, :read_length => :rid_count
    end

    class StringResponse < BinData::Record
      endian      :big
      int32       :response_len
      string      :response, :read_length => :response_len
    end

    class Deserializer7
      @@string_matcher = /^"[^"]*"$/


      def deserialize(text)
        records = []

        # Get rid of server signature
        binstr = text.gsub(/^\x00\x00\x00.{2,68}\)/, '')
        return { session: nil, message_content: records } if binstr.length < 5 # shortest valid response
        binstr = binstr[1..-1]
        binstr, session = grab(binstr, 4, true)
        binstr, result_type = grab(binstr, 1)
        # puts "#{result_type} --> #{binstr}"

        case result_type

        when 'l' # a collection of records
          binstr, col_len = grab(binstr, 4, true)
          qr = QueryRecord.new
          col_len.times do
            fields = { :document => {}, :structure => {} }
            struct_info = {}
            bytes_consumed = 0
            record = qr.read(binstr)
            bytes_consumed += record.do_num_bytes

            if record.record_type.do_num_bytes > 0
              fields[:record_type] = record.record_type.chr
            end
            if record.cluster_id.do_num_bytes > 0
              fields[:rid] = OrientDBClient::Rid.new("##{record.cluster_id}:#{record.cluster_position}")
            end

            # Parse the document proper
            tokens = record.properties.split(',')
            while token = tokens.shift
              # case of class
              arr = token.split('@', 2)
              if arr.length == 2
                fields[:class] = arr[0]
                token = arr[1]
              end
              field, value = parse_field(token, tokens, struct_info)
              fields[field] = value if field == :class  # unless field == :delimiter || field == :property_len

              fields[:document][field] = value
              fields[:structure][field] = struct_info[:type]
            end

            records << fields

            binstr = binstr[bytes_consumed .. -1]
          end

        when 'n'
          return { session: session, message_content: nil }

        when 'a'
          resp = OrientDBClient::Deserializers::StringResponse.new.read(binstr)
          return { session: session, message_content: resp[:response] }

        when 'r'
          # TODO for single record
            puts 'We may not need implement this, but TODO for sure'
        end

        { session: session, message_content: records }
      end

      def grab(binary_string, n_bytes, to_int = false)
        slice = binary_string[0..(n_bytes -1)]
        slice = slice.unpack("H*").first.to_i(16) if to_int

        [binary_string[n_bytes .. -1], slice]
      end

      private

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
          value =  parse_rid_bag(value)
        else
          value = parse_value(value, tokens, struct_info)
        end

        return field, value
      end

      def parse_rid_bag(value)
        value = Base64.decode64(value)
        return '' if value.length < 1
        rb = RidBag.new.read value

        return [] if rb.rid_count < 1
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
            value = Base64.decode64(remove_ends(value))
          when '#'
            struct_info[:type] = :rid
            value = OrientDBClient::Rid.new(value)
          else

            value = if value.length == 0
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