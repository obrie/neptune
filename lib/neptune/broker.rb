require 'forwardable'
require 'thread'
require 'neptune/connection'
require 'neptune/support/assertions'
require 'neptune/support/counter'
require 'neptune/support/pretty_print'
require 'neptune/resource'

module Neptune
  # A node within a Kafka cluster
  class Broker < Resource
    include Support::Assertions
    include Support::PrettyPrint
    extend Forwardable

    # The broker's unique identifier
    # @return [Fixnum]
    attribute :id, Int32

    # The broker's hostname
    # @return [String]
    attribute :host, String

    # The broker's port number
    # @return [Fixnum]
    attribute :port, Int32

    # The connection being used by the broker
    # @return [Neptune::Connection]
    attr_reader :connection

    # The cluster this broker belongs to
    # @return [Neptune::Cluster]
    attr_accessor :cluster

    delegate [:config] => :cluster

    def initialize(*) #:nodoc:
      super
      @lock = Mutex.new
      @correlation_id = Support::Counter.new
      @connection = Connection.new(host, port)
    end

    def cluster=(cluster) #:nodoc:
      @cluster = cluster
      @connection.config = cluster.config
    end

    # The URI for this broker
    # @return [String]
    def uri
      "#{host}:#{port}"
    end

    # Sets the URI for this broker
    def uri=(value)
      host, port = value.split(':')
      self.host = host
      self.port = port
    end

    # Invokes the produce API with the given requests
    #
    # @param [Array<String, Neptune::Api::Produce::Request>] requests Messages to send for each topic/partition
    # @return [Neptune::Api::Produce::BatchResponse]
    def produce(requests, options = {})
      assert_valid_keys(options, :required_acks, :ack_timeout)

      request = Api::Produce::BatchRequest.new(
        required_acks: options.fetch(:required_acks, config.required_acks),
        ack_timeout: options.fetch(:ack_timeout, config.ack_timeout),
        requests: requests
      )

      if request.required_acks != 0
        write(request, Api::Produce::BatchResponse)
      else
        write(request)

        Api::Produce::BatchResponse.new(responses: requests.map do |request|
          Api::Produce::Response.new(
            topic_name: request.topic_name,
            partition_id: request.partition_id,
            error_code: ErrorCode[:no_error]
          )
        end)
      end
    end

    # Fetch metadata for the given topics
    #
    # @param [Array<String>] topic_names The list of topics to fetch
    # @return [Neptune::Api::Metadata::Response]
    def metadata(topic_names, options = {})
      assert_valid_keys(options)

      request = Api::Metadata::Request.new(
        topic_names: topic_names
      )
      write(request, Api::Metadata::Response)
    end

    # Invokes the fetch API with the given requests
    #
    # @param [Array<Neptune::Api::Fetch::Request>] requests Topics/partitions to fetch messages from
    # @return [Neptune::Api::Fetch::BatchResponse]
    def fetch(requests, options = {})
      assert_valid_keys(options, :max_wait_time, :min_bytes)

      request = Api::Fetch::BatchRequest.new(
        max_wait_time: options.fetch(:max_wait_time, config.max_fetch_time),
        min_bytes: options.fetch(:min_bytes, config.min_fetch_bytes),
        requests: requests
      )
      write(request, Api::Fetch::BatchResponse)
    end

    # Invokes the offset API with the given requests
    #
    # @param [Array<Neptune::Api::Offset::Request>] requests Topics/partitions to look up offsets for
    # @return [Neptune::Api::Offset::BatchResponse]
    def offset(requests, options = {})
      assert_valid_keys(options)

      request = Api::Offset::BatchRequest.new(
        requests: requests
      )
      write(request, Api::Offset::BatchResponse)
    end

    # Fetch metadata for the given consumer group
    #
    # @param [String] group The group to fetch metadata for
    # @return [Neptune::Api::ConsumerMetadata::Response]
    def consumer_metadata(options = {})
      assert_valid_keys(options, :group)

      request = Api::ConsumerMetadata::Request.new(
        consumer_group: options.fetch(:group, config.consumer_group)
      )
      write(request, Api::ConsumerMetadata::Response)
    end

    # Invokes the offset fetch API with the given requests
    #
    # @param [Array<Neptune::Api::OffsetFetch::Request>] requests Topics/partitions to look up offsets for
    # @return [Neptune::Api::OffsetFetch::BatchResponse]
    def offset_fetch(requests, options = {})
      assert_valid_keys(options, :group)

      request = Api::OffsetFetch::BatchRequest.new(
        consumer_group: options.fetch(:group, config.consumer_group),
        requests: requests
      )
      write(request, Api::OffsetFetch::BatchResponse)
    end

    # Invokes the offset commit API with the given requests
    #
    # @param [Array<Neptune::Api::OffsetCommit::Request>] requests Topics/partitions to commit offsets for
    # @return [Neptune::Api::OffsetCommit::BatchResponse]
    def offset_commit(requests, options = {})
      assert_valid_keys(options, :group, :group_generation_id, :consumer_id, :retention_time)

      request = Api::OffsetCommit::BatchRequest.new(
        consumer_group: options.fetch(:group, config.consumer_group),
        consumer_group_generation_id: options.fetch(:group_generation_id, config.consumer_group_generation_id),
        consumer_id: options.fetch(:consumer_id, config.consumer_id),
        retention_time: options.fetch(:retention_time, config.offset_retention_time),
        requests: requests
      )
      write(request, Api::OffsetCommit::BatchResponse)
    end

    # Close any open connections to the broker
    # @return [Boolean] true, always
    def close
      @lock.synchronize { @connection.close }
      true
    end

    private
    # Writes the given request to the connection
    def write(request, response_class = nil)
      request.client_id = config.client_id
      request.correlation_id = @correlation_id.increment!
      request.api_version = config.api_version(request.class.api_name)

      # Write the request, ensuring that reads occur on the same connection
      connection = valid_connection
      connection.write(request.to_kafka(version: request.api_version))

      if response_class
        # Read a response from the connection
        buffer = connection.read(request.correlation_id)
        response_class.from_kafka(buffer, version: config.api_version(response_class.api_name))
      end
    end

    # Fetches the current, valid connection for this broker
    # @return [Neptune::Connection]
    def valid_connection
      # Check validity without locking to optimize access time
      @connection.valid? || @lock.synchronize do
        unless @connection.valid?
          @connection = Connection.new(host, port, config)
        end
      end

      @connection
    end

    def pretty_print_ignore #:nodoc:
      [:@cluster, :@connection]
    end
  end
end

require 'neptune/api'
