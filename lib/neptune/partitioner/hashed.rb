require 'zlib'

module Neptune
  module Partitioner
    # Partitions keys using a consistent hash function based on the total
    # number of partitions
    class Hashed
      # Generates the hashed partition index.  This will use all partitions
      # even if some are not available to ensure that those messages are
      # always published to the same partition.
      # @return [Fixnum]
      def call(key, available, total)
        Zlib.crc32(key) % total
      end
    end
  end
end