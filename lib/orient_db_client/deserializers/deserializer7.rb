require_relative '../rid'
require 'bindata'
require 'base64'

module OrientDbClient
  module Deserializers

    class QueryRecord < BinData::Record
      endian :big

      int16 						:delimiter,		:value => 0
      int8 						  :type_not_sure
      int16 						:data1
      int64         		:data2
      int32         		:data3
      int32             :property_len
      string            :properties, :read_length => :property_len
    end

    class Deserializer7
      @@string_matcher = /^"[^"]*"$/


      def deserialize(text)
        records = []
        struct_info = {}

        # Get rid of server signature
        binstr = text.gsub(/^\x00\x00\x00.{2,68}\)/, '')

        binstr = binstr[1..-1]
        binstr, session = grab(binstr, 4, true)
        binstr, result_type = grab(binstr, 1)

        case result_type

        when 'l' # a collection of records
          binstr, col_len = grab(binstr, 4, true)
          qr = QueryRecord.new
          col_len.times do
            puts 'Working a record...'
            bytes_consumed = 0
            record = qr.read(binstr)
            bytes_consumed += record.do_num_bytes
            fields = {}
            tokens = record.properties.split(',')
            while token = tokens.shift
              field, value = parse_field(token, tokens, struct_info)
              fields[field] = value
            end
            records << {record => fields}
            binstr = binstr[bytes_consumed .. -1]
          end


        when 'r'
          # do for single record
        end

        { session: session, message_content: records }
      end

      def grab(binary_string, n_bytes, to_int = false)
        slice = binary_string[0..(n_bytes -1)]
        slice = slice.unpack("H*").first.to_i(16) if to_int

        [binary_string[n_bytes .. -1], slice]
      end

      # def old_deserialize(text) # Perhaps this one is document based - TODO
      #     result = { :document => {}, :structure => {} }
      #     struct_info = {}
      #
      #     serialized_record = text
      #
      #     if m = serialized_record.match(/([^,@]+)@/)
      #         result[:class] = m[1]
      #         serialized_record.gsub!(/^[^@]*@/, '')
      #     end
      #
      #     tokens = serialized_record.split(",")
      #
      #     while token = tokens.shift
      #         field, value = parse_field(token, tokens, struct_info)
      #
      #         result[:document][field] = value
      #         result[:structure][field] = struct_info[:type]
      #     end
      #
      #     result
      # end

      private

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
        field, value = token.split(":", 2)

        field = remove_ends(field) if field.match(@@string_matcher)

        value = parse_value(value, tokens, struct_info)

        return field, value
      end

      def parse_time(value)
        Time.at(value[0...value.length - 1].to_i).utc
      end

      def parse_value(value, tokens, struct_info = {})
        struct_info[:type] = nil

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
          when '{'
            close_token!(value, '}', ',', tokens)

            struct_info[:type] = :map

            value = deserialize(remove_ends(value))[:document]
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
            value = OrientDbClient::Rid.new(value)
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