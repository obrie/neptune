require 'neptune/connection'
require 'neptune/helpers/assertions'
require 'neptune/helpers/pretty_print'
require 'neptune/resource'

module Neptune
  # A node within a Kafka cluster
  class Broker < Resource
    include Helpers::Assertions
    include Helpers::PrettyPrint

    # The broker's unique identifier
    # @return [Fixnum]
    attribute :id, Int32

    # The broker's hostname
    # @return [String]
    attribute :host, String

    # The broker's port number
    # @return [Fixnum]
    attribute :port, Int32

    # The cluster this broker belongs to
    # @return [Neptune::Cluster]
    attr_accessor :cluster

    def initialize(*) #:nodoc:
      super
      @correlation_id = 0
    end

    # The connection being used by the broker
    # @return [Neptune::Connection]
    def connection
      @connection ||= Connection.new(host, port, cluster.config)
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
        client_id: cluster.config.client_id,
        required_acks: options.fetch(:required_acks, cluster.config.required_acks),
        ack_timeout: options.fetch(:ack_timeout, cluster.config.ack_timeout),
        requests: requests
      )

      write(request)

      if request.required_acks != 0
        read(Api::Produce::BatchResponse)
      else
        Api::Produce::BatchResponse.new(responses: requests.map do |request|
          Api::Produce::Response.new(
            topic_name: request.topic_name,
            partition_id: request.partition_id,
            error_code: ErrorCode.find_by_name(:no_error)
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
        client_id: cluster.config.client_id,
        topic_names: topic_names
      )
      write(request)
      read(Api::Metadata::Response)
    end

    # Invokes the fetch API with the given requests
    #
    # @param [Array<Neptune::Api::Fetch::Request>] requests Topics/partitions to fetch messages from
    # @return [Neptune::Api::Fetch::BatchResponse]
    def fetch(requests, options = {})
      assert_valid_keys(options, :max_time, :min_bytes)

      request = Api::Fetch::BatchRequest.new(
        client_id: cluster.config.client_id,
        max_wait_time: options.fetch(:max_time, cluster.config.max_fetch_time),
        min_bytes: options.fetch(:min_bytes, cluster.config.min_fetch_bytes),
        requests: requests
      )
      write(request)
      read(Api::Fetch::BatchResponse)
    end

    # Invokes the offset API with the given requests
    #
    # @param [Array<Neptune::Api::Offset::Request>] requests Topics/partitions to look up offsets for
    # @return [Neptune::Api::Offset::BatchResponse]
    def offset(requests, options = {})
      assert_valid_keys(options)

      request = Api::Offset::BatchRequest.new(
        client_id: cluster.config.client_id,
        requests: requests
      )
      write(request)
      read(Api::Offset::BatchResponse)
    end

    # Fetch metadata for the given consumer group
    #
    # @param [String] group The group to fetch metadata for
    # @return [Neptune::Api::ConsumerMetadata::Response]
    def consumer_metadata(options = {})
      assert_valid_keys(options, :group)

      request = Api::ConsumerMetadata::Request.new(
        consumer_group: options.fetch(:group, cluster.config.consumer_group)
      )
      write(request)
      read(Api::ConsumerMetadata::Response)
    end

    # Invokes the fetch offset API with the given requests
    #
    # @param [Array<Neptune::Api::OffsetFetch::Request>] requests Topics/partitions to look up offsets for
    # @return [Neptune::Api::OffsetFetch::BatchResponse]
    def consumer_offset(requests, options = {})
      assert_valid_keys(options, :group)

      request = Api::OffsetFetch::BatchRequest.new(
        client_id: cluster.config.client_id,
        consumer_group: options.fetch(:group, cluster.config.consumer_group),
        requests: requests
      )
      write(request)
      read(Api::OffsetFetch::BatchResponse)
    end

    # Close any open connections to the broker
    # @return [Boolean] true, always
    def close
      connection.close
      true
    end

    private
    def next_correlation_id #:nodoc:
      @correlation_id += 1
    end

    # Reads a response from the connection
    def read(response_class)
      connection.verify
      response_class.from_kafka(connection.read)
    end

    # Writes the given request to the connection
    def write(request)
      request.client_id = cluster.config.client_id
      request.correlation_id = next_correlation_id

      connection.verify
      connection.write(request.to_kafka)
    end

    def pretty_print_ignore #:nodoc:
      [:@cluster, :@connection]
    end
  end
end

require 'neptune/api'
