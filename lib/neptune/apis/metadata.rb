require 'neptune/request'
require 'neptune/response'
require 'neptune/broker'
require 'neptune/topic'

module Neptune
  class MetadataRequest < Request
    # List of topics to fetch metadata for
    # @return [Array<String>]
    attribute :topic_names, ArrayOf[String]

    def initialize(*) #:nodoc:
      super
      self.api_key = 3
    end
  end

  class MetadataResponse < Response
    # Brokers used for the requested topics
    # @return [Array<Neptune::Broker]
    attribute :brokers, ArrayOf[Broker]

    # Topics found in the cluster
    # @return [Array<Neptune::Topic]
    attribute :topics, ArrayOf[Topic]
  end
end