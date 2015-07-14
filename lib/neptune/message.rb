require 'neptune/resource'

module Neptune
  # An individual message
  class Message < Resource
    # The offset used in Kafka as the log sequence number. When sending messages
    # the offset is ignored.
    # @return [Fixnum]
    attribute :offset, Int64

    # The size of the message
    # @return [Fixnum]
    attribute :size, Int32

    # The CRC32 of the remainder of the message bytes. This is used to check the
    # integrity of the message on the broker and consumer.
    # @return [Fixnum]
    attribute :checksum, Int32

    # Used to allow backwards compatible evolution of the message binary format.
    # The current value is 0
    # @return [Fixnum]
    attribute :version_id, Int8

    # Holds metadata attributes about the message. The lowest 2 bits contain the
    # compression codec used for the message. The other bits should be set to 0.
    # @return [Fixnum]
    attribute :attributes, Int8

    # An optional message key used for partition assignment
    # @return [String]
    attribute :key, Bytes

    # The actual message contents
    # @return [String]
    attribute :value, Bytes

    # Whether the checksum failed on this message
    # @return [Boolean]
    attr_accessor :checksum_failed
    def checksum_failed?
      @checksum_failed
    end
  end
end