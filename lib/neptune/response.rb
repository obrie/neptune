require 'neptune/resource'

module Neptune
  # A base response from Kafka
  class Response < Resource
    # Client-supplied integer for matching request and response between the
    # client and server
    # @return [Fixnum]
    attribute :correlation_id, Int32
  end
end