require 'neptune/error_code'
require 'neptune/resource'

module Neptune
  # A base response from Kafka
  class Response < Resource
    # Client-supplied integer for matching request and response between the
    # client and server
    # @return [Fixnum]
    attribute :correlation_id, Int32

    # Whether all the request was successful
    # @return [Boolean]
    def success?
      enumerable? ? all?(&:success?) : true
    end

    # The first available error in the response
    # @return [Fixnum]
    def error_code
      enumerable? && map(&:error_code).reject(&:success?).first || ErrorCode[:no_error]
    end

    private
    def enumerable? #:nodoc:
      is_a?(Enumerable)
    end
  end
end