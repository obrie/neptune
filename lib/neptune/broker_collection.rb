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

    # Enumerates over each broker
    def each
      @by_id.each {|id, broker| yield broker}
    end
  end
end