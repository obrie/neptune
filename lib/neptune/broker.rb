require 'neptune/resource'

module Neptune
  # A node within a Kafka cluster
  class Broker < Resource
    # The broker's unique identifier
    # @return [Fixnum]
    attribute :id, Int32

    # The broker's hostname
    # @return [String]
    attribute :host, Types::String

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
      @connection = cluster.connections[uri]
    end

    # The URI for this broker
    def uri
      "#{host}:#{port}"
    end

    # Sets the URI for this broker
    def uri=(value)
      host, port = value.split(':')
      self.host = host
      self.port = port
    end

    # Invokes the produce API with the given topic messages
    #
    # @param [Array<Neptune::TopicMessage>] topic_messages Messages to send
    # @return [ProduceResponse]
    def produce(topic_messages)
      request = ProduceRequest.new(
        :required_acks => config[:required_acks],
        :ack_timeout => config[:ack_timeout],
        :topic_messages => topic_messages
      )

      write(request)

      if request.required_acks != 0
        read(ProduceResponse)
      else
        true
      end
    end

    # Fetch metadata for the given topics
    #
    # @param [Array<String>] topic_names The list of topics to fetch
    # @return [MetadataResponse]
    def metadata(topic_names)
      request = MetadataRequest.new(:topic_names => topic_names)
      write(request)
      read(MetadataResponse)
    end

    # Close any open connections to the broker
    def close
      connection.close
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
  end
end

require 'neptune/apis/metadata'
require 'neptune/apis/produce'