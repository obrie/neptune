require 'neptune/resource'
require 'neptune/compression'

module Neptune
  # An individual message
  class Message < Resource
    # Last 3 bits are used to indicate compression
    METADATA_COMPRESSION = 0x7

    # The offset used in Kafka as the log sequence number. When sending messages
    # the offset is ignored.
    # @return [Fixnum]
    attribute :offset, Int64

    # The CRC32 of the remainder of the message bytes. This is used to check the
    # integrity of the message on the broker and consumer.
    # @return [Fixnum]
    attribute :checksum, Checksum

    # Used to allow backwards compatible evolution of the message binary format.
    # The current value is 0
    # @return [Fixnum]
    attribute :version_id, Int8

    # Holds metadata attributes about the message. The lowest 2 bits contain the
    # compression codec used for the message. The other bits should be set to 0.
    # @return [Fixnum]
    attribute :metadata, Int8

    # An optional message key used for partition assignment
    # @return [String]
    attribute :key, Bytes

    # The actual message contents
    # @return [String]
    attribute :value, Bytes

    # The partition this message belongs to
    # @return [Neptune::Partition]
    attr_accessor :partition

    def initialize(*) #:nodoc:
      super
      @version_id ||= 0
      @offset ||= 0
      @metadata ||= 0
    end

    # The topic this message belongs to
    # @return [Neptune::Topic]
    def topic
      partition && partition.topic
    end

    # Compresses the current message's value with the given codec
    # @return [Boolean] true, always
    def compress(codec)
      self.compression_codec_id = codec.id
      self.value = codec.compress(value)
      true
    end

    # ID of the codec used to compress this message
    # @return [Fixnum]
    def compression_codec_id
      metadata & METADATA_COMPRESSION
    end

    # Sets the codec being used to compress this message
    def compression_codec_id=(value)
      self.metadata = metadata | value
    end

    # Codec used to compress this message
    # @return [Class]
    def compression_codec
      Compression.find_by_id(compression_codec_id)
    end

    # Whether the value is compressed
    # @return [Boolean]
    def compressed?
      compression_codec_id > 0
    end

    # Decompresses the value in the message with the configured compression codec
    # @return [String] decompressed value
    def decompressed_value
      compression_codec.decompress(value)
    end
  end
end