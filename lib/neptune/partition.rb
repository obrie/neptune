require 'neptune/resource'

module Neptune
  # An ordered, immutable sequence of messages within a topic
  class Partition < Resource
    # The error code when loading metadata for the partition
    # @return [Fixnum]
    attribute :error_code, Int16

    # The unique identifier for the partition
    # @return [Fixnum]
    attribute :id, Int32

    # The id of the broker currently acting as leader
    # @return [Fixnum]
    attribute :leader_id, Int32

    # The ids of the brokers that are acting as slaves for the leader
    # @return [Array<Fixnum>]
    attribute :replica_ids, [Int32]

    # The ids of replicas that are "caught up" to the leader
    # @return [Array<Fixnum>]
    attribute :synced_replica_ids, [Int32]

    # The topic this partition belongs to
    # @return [Neptune::Cluster]
    attr_accessor :topic

    # The broker currently acting as leader
    # @return [Neptune::Broker]
    def leader
      leader_id == -1 ? nil : cluster.broker(leader_id)
    end

    # The brokers that are acting as slaves for the leader
    # @return [Array<Neptune::Broker>]
    def replicas
      replica_ids.map {|id| cluster.broker(id)}
    end

    # The replicas that are "caught up" to the leader
    # @return [Array<Neptune::Broker>]
    def synced_replicas
      synced_replica_ids.map {|id| cluster.broker(id)}
    end

    # Whether the partition is available for access
    # @return [Boolean]
    def available?
      (error_name == :no_error || error_name == :replica_not_available) && leader
    end

    private
    def cluster #:nodoc:
      topic.cluster
    end
  end
end