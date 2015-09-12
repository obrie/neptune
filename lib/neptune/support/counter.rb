require 'thread'

module Neptune
  module Support
    # Provides thread-safety for tracking counters
    class Counter
      # The current value of the counter
      # @return [Fixnum]
      attr_accessor :value

      def initialize(value = nil) #:nodoc:
        @value = value
        @lock = Mutex.new
      end

      # Increments the current value of the counter by 1, returning the result.
      # If the maximum is reached, then the counter will reset to 0.
      # @return [Fixnum]
      def increment!(max = 2147483647)
        @lock.synchronize do
          if !@value || @value == max - 1
            @value = 0
          else
            @value += 1
          end
        end
      end
    end
  end
end