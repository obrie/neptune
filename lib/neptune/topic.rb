require 'neptune/compression'
require 'neptune/error_code'
require 'neptune/errors'
require 'neptune/partition'
require 'neptune/resource'

module Neptune
  # A category or feed name to which messages are published
  class Topic < Resource
    # The error code when loading metadata for the partition
    # @return [Neptune::ErrorCode]
    attribute :error_code, ErrorCode

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
      @partition_counter = 0
    end

    # The compression codec being used in this topic
    # @return [Class]
    def compression_codec
      Compression.find_by_name(cluster.config.compression_codec) if compressed?
    end

    # Whether this topic is compressed
    # @return [Boolean]
    def compressed?
      cluster.config.compressed_topics.include?(name)
    end

    # Whether this topic exists in the cluster
    # @return [Boolean]
    def exists?
      error_code.success?
    end

    # Whether a leader is available for this topic
    # @return [Boolean]
    def leader_available?
      error_code != :leader_not_available
    end

    # Looks up the partition with the given id
    # @return [Neptune::Partition]
    def partition(id)
      partitions.detect {|partition| partition.id == id}
    end

    # Looks up the partition with the given id or raises an error if it doesn't exist
    # @return [Neptune::Partition]
    def partition!(id)
      partition(id) || ErrorCode[:unknown_topic_or_partition].raise
    end

    # Looks up the partitions that are currently available for access
    # @return [Array<Neptune::Partition>]
    def available_partitions
      partitions.select(&:available?)
    end

    # Determines the partition for the given key
    # @return [Fixnum]
    def partition_for!(key)
      if leader_available?
        if key
          # Use the configured partitioner
          partition_id = cluster.config.partitioner.call(key, partitions.count)
          partition!(partition_id)
        elsif available_partitions.any?
          # Round-robin between partitions
          available_partitions[next_partition_counter % available_partitions.count]
        else
          ErrorCode[:leader_not_available].raise
        end
      end
    end

    # Publish a value to this topic
    # @return [Neptune::Produce::BatchResponse]
    def produce(value, options = {}, &callback)
      cluster.produce(name, value, options, &callback)
    end

    # Publish a value to this topic or raise an exception if it fails
    # @return [Neptune::Produce::BatchResponse]
    def produce!(*args, &callback)
      cluster.produce!(name, *args, &callback)
    end

    private
    # Counter used to round-robin between partitions
    # @return [Fixnum]
    def next_partition_counter
      @partition_counter += 1
    end

    def pretty_print_ignore #:nodoc:
      [:@cluster]
    end
  end
end