require 'zlib'
require 'neptune/connection'

module Neptune
  # Encapsulates configuration information for a client
  class Config
    # The unique identifier for this client (for debugging purposes)
    # @return [String]
    attr_accessor :client_id

    # The number of times to retry before failing API calls
    # @return [Fixnum]
    attr_accessor :retry_count

    # The amount of time to wait (in ms) between retries
    # @return [Fixnum]
    attr_accessor :retry_backoff

    # The amount of time to wait (in ms) until timing out on reads
    # @return [Fixnum]
    attr_accessor :read_timeout

    # The amount of time to wait (in ms) until timing out on writes
    # @return [Fixnum]
    attr_accessor :write_timeout

    # The amount of time to wait (in ms) until timing out on connect
    # @return [Fixnum]
    attr_accessor :connect_timeout

    #
    # Produce-specific configurations
    #

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

    # The number of acknowledgements required for a produced message to be
    # considered successful
    # @return [Fixnum]
    attr_accessor :required_acks

    # The amount of time to wait (in ms) until timing out on receiving an
    # acknowledgement from a broker's followers
    # @return [Fixnum]
    attr_accessor :ack_timeout

    #
    # Consumer-specific configurations
    #

    # The name of the group uniquely identifying consumer operations
    # @return [String]
    attr_accessor :consumer_group

    # The maximum amount of time to block waiting if insufficient data is
    # available at the time the request is issued
    # @return [Fixnum]
    attr_accessor :max_fetch_time

    # The minimum number of bytes of messages that must be available to give a
    # response. If set to 0, the server will always respond immediately.
    # @return [Fixnum]
    attr_accessor :min_fetch_bytes

    # The maximum bytes to include in messages for partition requests
    # @return [Fixnum]
    attr_accessor :max_fetch_bytes

    #
    # API version configurations
    #

    # The versions to use for each available API
    # @return [Hash<Symbol, Fixnum>]
    attr_accessor :api_versions

    def initialize(options = {}) #:nodoc:
      options = {
        client_id: 'neptune',
        retry_count: 3,
        retry_backoff: 100,
        required_acks: 0,
        ack_timeout: 1_500,
        read_timeout: 10_000,
        write_timeout: 10_000,
        connect_timeout: 10_000,

        # Producer configurations
        partitioner: lambda {|key, partition_count| Zlib::crc32(key) % partition_count},
        refresh_interval: 600_000,
        compressed_topics: [],
        compression_codec: :none,

        # Consumer configurations
        consumer_group: 'default',

        max_fetch_time: 100,
        min_fetch_bytes: 1,
        max_fetch_bytes: 1024 * 1024,

        api_versions: {}
      }.merge(options)

      # API configurations
      options[:api_versions] = {
        consumer_metadata: 0,
        fetch: 0,
        metadata: 0,
        offset: 0,
        offset_fetch: 1,
        produce: 0
      }.merge(options[:api_versions])

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

    # Looks up the version configured for the given API
    # @return [Fixnum]
    def api_version(api_name)
      api_versions.fetch(api_name)
    end
  end
end