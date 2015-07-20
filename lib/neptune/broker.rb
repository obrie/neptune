require 'neptune/resource'

module Neptune
  # A node within a Kafka cluster
  class Broker < Resource
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

    # The connection being used by the broker
    # @return [Neptune::Connection]
    attr_reader :connection

    def initialize(*) #:nodoc:
      super
      @correlation_id = 0
    end

    # The connection to use for this broker
    # @return [Neptune::Connection]
    def connection
      @connection ||= cluster.connections[uri]
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

    # Invokes the produce API with the given topic requests
    #
    # @param [Array<Neptune::Api::Produce::TopicRequest>] topic_requests Messages to send for each topic
    # @return [Neptune::Api::Produce::Response]
    def produce(topic_requests)
      request = Api::Produce::Request.new(
        client_id: cluster.config[:client_id],
        required_acks: cluster.config[:required_acks],
        ack_timeout: cluster.config[:ack_timeout],
        topic_requests: topic_requests
      )

      write(request)

      if request.required_acks != 0
        read(Api::Produce::Response)
      else
        Api::Produce::Response.new
      end
    end

    # Fetch metadata for the given topics
    #
    # @param [Array<String>] topic_names The list of topics to fetch
    # @return [Neptune::Api::Metadata::Response]
    def metadata(topic_names)
      request = Api::Metadata::Request.new(
        client_id: cluster.config[:client_id],
        :topic_names => topic_names
      )
      write(request)
      read(Api::Metadata::Response)
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
      [:'@cluster']
    end
  end
end

require 'neptune/api/metadata'
require 'neptune/api/produce'