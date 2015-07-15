require 'neptune/request'
require 'neptune/response'
require 'neptune/broker'
require 'neptune/topic'

module Neptune
  class MetadataRequest < Request
    # @return [Array<String>]
    attribute :topic_names, ArrayOf[String]

    def initialize(*) #:nodoc:
      super
      self.api_key = 3
    end
  end

  class MetadataResponse < Response
    # @return [Array<Neptune::Broker]
    attribute :brokers, ArrayOf[Broker]

    # @return [Array<Neptune::Topic]
    attribute :topics, ArrayOf[Topic]
  end
end