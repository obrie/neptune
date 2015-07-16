require 'neptune/types/int16'

module Neptune
  # Provides typecasting for Error data
  class ErrorCode
    NAMES_BY_VALUE = {
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

    # Converts the given value to its Kafka format
    # @return [String]
    def self.to_kafka(value, *)
      Types::Int16.to_kafka(value)
    end

    # Converts from the Kafka data in the current buffer's position
    # @return [Fixnum]
    def self.from_kafka(buffer)
      new(Types::Int16.from_kafka(buffer))
    end

    # The numeric id
    # @return[Fixnum]
    attr_reader :id

    def initialize(id) #:nodoc:
      @id = id
    end

    # Determines whether this error is one of the given values (ids or names)
    # @return [Boolean]
    def is?(*values)
      values.any? {|value| self == value}
    end

    # The human-readable name for the error code
    # @return [String]
    def name
      NAMES_BY_VALUE[id]
    end

    # Determines whether this error is equal to another based on their id.
    # 
    # @param [Object] other The object this resource is being compared against
    # @return [Boolean] +true+ if the error codes are equal, otherwise +false+
    def ==(other)
      case other
      when ErrorCode
        other.id == id
      when Fixnum
        other == id
      when Symbol
        other == name
      else
        false
      end
    end
    alias :eql? :==

    # Generates a hash for this resource based on the id
    # @return [Fixnum]
    def hash
      id.hash
    end
  end
end