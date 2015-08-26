require 'neptune/message'
require 'neptune/resource'

module Neptune
  module Api
    module Fetch
      class Response < Resource
        # The partition this result corresponds to
        # @return [String]
        attribute :topic_name, Index[String]

        # The partition this result corresponds to
        # @return [Fixnum]
        attribute :partition_id, Int32

        # The error from this partition
        # @return [Neptune::ErrorCode]
        attribute :error_code, ErrorCode

        # The offset at the end of the log for this partition
        # @return [Fixnum]
        attribute :highwater_mark_offset, Int64

        # The messages fetched
        # @return [Array<Neptune::Message>]
        attribute :messages, SizeBoundArrayOf[Message]

        delegate [:success?, :retriable?] => :error_code

        private
        # Writes to the given attribute from a Kafka buffer
        # @private
        def write_kafka_attribute(attr, buffer, context = {})
          result = super

          # Set the underlying messages associated with this partition,
          # decompressing any that might have been previously compressed.
          if attr == :messages
            decompress(context) if compressed?
          end

          result
        end

        # Whether any of the underlying messages are compressed
        # @return [Boolean]
        def compressed?
          messages.any?(&:compressed?)
        end

        # Decompresses the current messages
        # @return [Boolean] true, always
        def decompress(context)
          type = self.class.attributes[:messages]
          decompressed_messages = []

          messages.each do |message|
            if message.compressed?
              # Inflate the underlying messages
              buffer = Buffer.new(message.decompressed_value)
              buffer.prepend(Types::Size.to_kafka(nil, buffer: buffer))
              decompressed_messages.concat(type.from_kafka(buffer, context))
            else
              decompressed_messages << message
            end
          end

          self.messages = decompressed_messages

          true
        end
      end
    end
  end
end