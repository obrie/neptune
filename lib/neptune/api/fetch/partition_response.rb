require 'neptune/buffer'
require 'neptune/message'
require 'neptune/resource'

module Neptune
  module Api
    module Fetch
      class PartitionResponse < Resource
        # The partition this response corresponds to
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

        # Whether the fetch was successful for this partition
        # @return [Boolean]
        def success?
          error_code == :no_error
        end

        # Sets the underlying messages associated with this partition, decompressing
        # any that might have been previously compressed.
        def messages=(messages)
          @messages = messages
          decompress if compressed?
        end

        private
        # Whether any of the underlying messages are compressed
        # @return [Boolean]
        def compressed?
          messages.any?(&:compressed?)
        end

        # Decompresses the current messages
        # @return [Boolean] true, always
        def decompress
          type = self.class.attributes[:messages]
          decompressed_messages = []

          messages.each do |message|
            if message.compressed?
              # Inflate the underlying messages
              buffer = Buffer.new(message.decompressed_value)
              buffer.prepend(Types::Size.to_kafka(nil, buffer))
              decompressed_messages.concat(type.from_kafka(buffer))
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