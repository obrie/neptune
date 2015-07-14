require 'neptune/request'
require 'neptune/response'
require 'neptune/broker'
require 'neptune/topic'

module Neptune
  class MetadataRequest < Request
    # @return [Array<String>]
    attribute :topic_names, [String]

    def initialize(*) #:nodoc:
      super
      self.api_key = 3
    end
  end

  class MetadataResponse < Response
    # @return [Array<Neptune::Broker]
    attribute :brokers, [Broker]

    # @return [Array<Neptune::Topic]
    attribute :topics, [Topic]
  end
end