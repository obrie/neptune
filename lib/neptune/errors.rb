module Neptune
  class Error < StandardError
    attr_reader :cause

    def initialize(message = nil, cause = $!) #:nodoc:
      super(message)
      @cause = cause
      set_backtrace(cause.backtrace) if cause
    end
  end

  class APIError < Error
    # Error code that came back from the cluster
    # @return [Neptune::ErrorCode]
    attr_reader :error_code

    def initialize(error_code, *args) #:nodoc:
      super("Received error from server: #{error_code.name} (#{error_code.id})", *args)
    end
  end

  # Data parse errors
  class EncodingError < Error;end
  class DecodingError < Error;end

  # Connection errors
  class ConnectionError < Error; end
end