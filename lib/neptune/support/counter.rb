require 'thread'

module Neptune
  module Support
    # Provides thread-safety for tracking counters
    class Counter
      # The current value of the counter
      # @return [Fixnum]
      attr_accessor :value

      def initialize(value = 0) #:nodoc:
        @value = value
        @lock = Mutex.new
      end

      # Increments the current value of the counter by 1, returning the result
      # @return [Fixnum]
      def increment!
        @lock.synchronize { @value += 1 }
      end
    end
  end
end