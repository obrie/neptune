require 'neptune/buffer'
require 'neptune/message'
require 'neptune/resource'

module Neptune
  # A set of messages for a given partition
  class PartitionMessage < Resource
    # The partition this message belongs to
    # @return [Fixnum]
    attribute :partition_id, Int32

    # Only applicable when fetched
    # attribute :error_code, Int16
    # attribute :highwater_mark_offset, Int64

    # The actual collection of messages
    # @return [Array<Neptune::Message>]
    attribute :messages, SizeBoundArrayOf[Message]

    def initialize(*) #:nodoc:
      super
      @messages ||= []
    end

    # Sets the underlying messages associated with this partition, decompressing
    # any that might have been previously compressed.
    def messages=(messages)
      @messages = messages
      decompress if compressed?
    end

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

    # Whether the underlying messages are compressed
    # @return [Boolean]
    def compressed?
      messages.length == 1 && messages[0].compressed?
    end

    # Decompresses the current messages
    # @return [Boolean] true, always
    def decompress
      buffer = Buffer.new(messages[0].decompressed_value)
      buffer.prepend(Types::Size.to_kafka(nil, buffer))
      write_kafka_attribute(:messages, buffer)

      true
    end
  end
end