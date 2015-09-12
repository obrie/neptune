require 'neptune/support/counter'

module Neptune
  module Partitioner
    # Partitions keys by cycling through available partitions
    class RoundRobin
      def initialize(*) #:nodoc:
        @counter = Support::Counter.new
      end

      # Cycles through to the next partition index
      # @return [Fixnum]
      def call(key, available, *)
        @counter.increment!(available)
      end
    end
  end
end