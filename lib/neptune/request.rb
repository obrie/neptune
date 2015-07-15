require 'neptune/resource'

module Neptune
  # A base request to Kafka
  class Request < Resource
    # The bytesize of the request
    # @return [Fixnum]
    attribute :size, Size

    # The id of the API being invoked
    # @return [Fixnum]
    attribute :api_key, Int16

    # The version of the API being invoked
    # @return [Fixnum]
    attribute :api_version, Int16

    # Client-supplied integer for matching request and response between the
    # client and server
    # @return [Fixnum]
    attribute :correlation_id, Int32

    # Client-supplied identifier for the application. The identifier will be
    # used when logging errors, monitoring aggregates, etc.
    # @return [String]
    attribute :client_id, String

    def initialize(*) #:nodoc:
      super
      self.api_version ||= 0
    end
  end
end