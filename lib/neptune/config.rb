require 'zlib'
require 'neptune/connection'

module Neptune
  # Encapsulates configuration information for a client
  class Config
    # The unique identifier for this client (for debugging purposes)
    # @return [String]
    attr_accessor :client_id

    # The topics that are configured to store compressed messages
    # @return [Array<String>]
    attr_accessor :compressed_topics

    # The codec being used to compress messages
    # @return [String]
    attr_accessor :compression_codec

    # The amount of time to wait (in ms) between refreshes of a cluster's
    # metadata
    # @return [Fixnum]
    attr_accessor :refresh_interval

    # The function used to partition messages based on key
    # @return [Proc]
    attr_accessor :partitioner

    # The number of times to retry before failing API calls
    # @return [Fixnum]
    attr_accessor :retry_count

    # The amount of time to wait (in ms) between retries
    # @return [Fixnum]
    attr_accessor :retry_backoff

    # The number of acknowledgements required for a produced message to be
    # considered successful
    # @return [Fixnum]
    attr_accessor :required_acks

    # The amount of time to wait (in ms) until timing out on receiving an
    # acknowledgement from a broker's followers
    # @return [Fixnum]
    attr_accessor :ack_timeout

    # The amount of time to wait (in ms) until timing out on reads
    # @return [Fixnum]
    attr_accessor :read_timeout

    # The amount of time to wait (in ms) until timing out on writes
    # @return [Fixnum]
    attr_accessor :write_timeout

    # The amount of time to wait (in ms) until timing out on connect
    # @return [Fixnum]
    attr_accessor :connect_timeout

    def initialize(options = {}) #:nodoc:
      options = {
        partitioner: lambda {|key, partition_count| Zlib::crc32(key) % partition_count},
        refresh_interval: 600_000,
        retry_count: 3,
        retry_backoff: 100,
        required_acks: 0,
        ack_timeout: 1_500,
        read_timeout: 10_000,
        write_timeout: 10_000,
        connect_timeout: 10_000,
        compressed_topics: []
      }.merge(options)
      options.each {|option, value| send("#{option}=", value)}
    end

    # Looks up the configuration option with the given key
    # @return [Object]
    def [](key)
      send(key)
    end

    # Generates a hash containing the given configuration options
    # @return [Hash]
    def slice(*keys)
      keys.each_with_object({}) {|key, h| h[key] = self[key]}
    end
  end
end