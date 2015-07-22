require 'neptune/message'
require 'neptune/resource'

module Neptune
  module Api
    module Produce
      class Request < Resource
        # The topic this message belongs to
        # @return [String]
        attribute :topic_name, Index[String]

        # The partition this message belongs to
        # @return [Fixnum]
        attribute :partition_id, Int32

        # The messages to produce
        # @return [Array<Neptune::Message>]
        attribute :messages, SizeBoundArrayOf[Message]

        # Compresses the individual messages with the given codec
        # @return [Boolean] true, always
        def compress(codec)
          buffer = Buffer.new(read_kafka_attribute(:messages))

          # Remove the size from the buffer since it'll get replaced by the new
          # messages size attribute
          Types::Int32.from_kafka(buffer)

          # Replace with a compressed message
          message = Message.new(value: buffer.read)
          message.compress(codec)
          @messages = [message]

          true
        end
      end
    end
  end
end