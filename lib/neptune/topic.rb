require 'neptune/partition'
require 'neptune/resource'

module Neptune
  # A category or feed name to which messages are published
  class Topic < Resource
    # The error code when loading metadata for the partition
    # @return [Fixnum]
    attribute :error_code, Int16

    # The name of the topic
    # @return [String]
    attribute :name, String

    # The list of partitions for the topic
    # @return [Array<Neptune::Partition>]
    attribute :partitions, ArrayOf[Partition] do
      partitions.each {|partition| partition.topic = self}
    end

    # The cluster this topic belongs to
    # @return [Neptune::Cluster]
    attr_accessor :cluster

    def initialize(*) #:nodoc:
      super
      @next_partition_counter = 0
    end

    # Whether this topic exists in the cluster
    # @return [Boolean]
    def exists?
      error_code == 0
    end

    # Whether a leader is available for this topic
    # @return [Boolean]
    def leader_available?
      error_name != :leader_not_available
    end

    # Looks up the partition with the given id
    # @return [Neptune::Partition]
    def partition(id)
      partitions.detect {|partition| partition.id == id}
    end

    # Determines the partition for the given key
    # @return [Fixnum]
    def partition_for(key)
      if leader_available?
        if key
          # Use the configured partitioner
          partition_id = cluster.config.partitioner.call(key, partitions.count)
          partition(partition_id) || raise(InvalidPartitionError.new("Partition #{partition_id} is invalid for Topic #{name}"))
        elsif available_partitions.any?
          # Round-robin between partitions
          available_partitions[next_partition_counter % available_partitions.count]
        end
      end
    end

    private
    # Looks up the partition with the given id
    # @return [Fixnum]
    def partition(id)
      partitions.detect {|partition| partition.id == id}
    end

    # Looks up the partitions that are currently available for access
    # @return [Array<Neptune::Partition>]
    def available_partitions
      partitions.select {|partition| partition.available?}
    end

    # Counter used to round-robin between partitions
    # @return [Fixnum]
    def next_partition_counter
      @partition_counter += 1
    end

    def pretty_print_ignore #:nodoc:
      [:'@cluster']
    end
  end
end