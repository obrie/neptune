require 'neptune/partitioner/hashed'
require 'neptune/partitioner/random'
require 'neptune/partitioner/round_robin'

module Neptune
  module Partitioner
    class << self
      # Looks up the partitioner associated with the given name
      # @return [Class]
      def find_by_name(name)
        @names.fetch(name)
      end

      # Registers a partitioner with the given name
      def register(name, partitioner)
        @names[name] = partitioner
      end
    end

    @names = {}

    register(:hashed, Hashed)
    register(:random, Random)
    register(:round_robin, RoundRobin)
  end
end