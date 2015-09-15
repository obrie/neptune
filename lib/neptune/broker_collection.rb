require 'neptune/broker'

module Neptune
  # Tracks a list of brokers for a cluster
  class BrokerCollection
    include Enumerable

    def initialize(cluster) #:nodoc:
      @cluster = cluster
      @by_id = {}
    end

    # Looks up the broker indexed by the given id
    # @return [Neptune::Broker]
    def [](id)
      @by_id[id]
    end

    # Indexes the broker with the given name
    def []=(id, broker)
      @by_id[id] = broker
    end

    # Creates a broker with the given attributes
    # @return [Neptune::Broker]
    def create(attrs)
      self << broker = Broker.new(attrs)
      broker
    end

    # Adds the given broker, syncing it with any broker that exists with the
    # same id
    # @return [Neptune::Broker]
    def <<(new_broker)
      if new_broker.id
        # Index by id, removing any existing index by uri
        broker = self[new_broker.id] ||= @by_id.delete(new_broker.uri) || new_broker
      else
        # Index by URI
        broker = self[new_broker.uri] ||= new_broker
      end
      broker.cluster = @cluster
      broker.attributes = new_broker.attributes unless broker == new_broker
      broker
    end

    # Replaces the current collection of brokers with the given ones.  Any
    # brokers that intersect will remain in the collection.
    # @return [Neptune::BrokerCollection]
    def replace(brokers)
      brokers = brokers.map {|broker| self << broker}

      ids_to_remove = @by_id.keys - brokers.map(&:id)
      ids_to_remove.each do |id|
        broker = @by_id.delete(id)
        broker.close
      end

      brokers
    end

    # Enumerates over each broker
    def each(&block)
      @by_id.values.each(&block)
    end
  end
end