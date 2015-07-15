require 'neptune/resource'

module Neptune
  class Request < Resource
    # @return [Fixnum]
    attribute :size, Int32

    # @return [Fixnum]
    attribute :api_key, Int16

    # @return [Fixnum]
    attribute :api_version, Int16

    # @return [Fixnum]
    attribute :correlation_id, Int32

    # @return [String]
    attribute :client_id, String

    def initialize(*) #:nodoc:
      super
      self.api_version ||= 0
    end
  end
end