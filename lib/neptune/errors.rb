module Neptune
  # Mapping of Kafka error codes to human-readable names
  ERROR_CODES = {
    -1 => :unknown_error,
    0 => :no_error,
    1 => :offset_out_of_range,
    2 => :invalid_message,
    3 => :unknown_topic_or_partition,
    4 => :invalid_message_size,
    5 => :leader_not_available,
    6 => :not_leader_for_partition,
    7 => :request_timed_out,
    8 => :broker_not_available,
    9 => :replica_not_available,
    10 => :message_size_too_large,
    11 => :stale_controller_epoch,
    12 => :offset_metadata_too_large,
    14 => :offsets_load_in_progress,
    15 => :consumer_coordinator_not_available,
    16 => :not_coordinator_for_consumer
  }

  class Error < StandardError
    attr_reader :inner_exception

    def initialize(message = nil, inner_exception = nil) #:nodoc:
      super(message)
      @inner_exception = inner_exception
    end
  end

  # Data parse errors
  class EncodingError < Error;end
  class DecodingError < Error;end

  # Connection errors
  class ConnectionError < Error; end

  # Lookup errors
  class InvalidPartitionError < Error; end
end