module Neptune
  module Partitioner
    # Partitions keys randomly based on the number of partitions available
    class Random
      # Generates a random partition index
      # @return [Fixnum]
      def call(key, available, *)
        rand(available)
      end
    end
  end
end