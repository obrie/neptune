module Neptune
  class Error < StandardError
    attr_reader :inner_exception

    def initialize(message = nil, inner_exception = nil) #:nodoc:
      super(message)
      @inner_exception = inner_exception
    end
  end

  class APIError < Error
    # Error code that came back from the cluster
    # @return [Neptune::ErrorCode]
    attr_reader :error_code

    def initialize(error_code, *args) #:nodoc:
      super("Received error from server: #{error_code}", *args)
    end
  end

  # Data parse errors
  class EncodingError < Error;end
  class DecodingError < Error;end

  # Connection errors
  class ConnectionError < Error; end

  # Lookup errors
  class InvalidTopicError < Error; end
  class InvalidPartitionError < Error; end
end