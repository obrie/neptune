require 'neptune/topic'

module Neptune
  # Tracks a list of topics for a cluster
  class TopicCollection
    include Enumerable

    def initialize(cluster) #:nodoc:
      @cluster = cluster
      @by_name = {}
    end

    # Looks up the topic indexed by the given name
    # @return [Neptune::Topic]
    def [](name)
      @by_name[name]
    end

    # Indexes the topic with the given name
    def []=(name, topic)
      @by_name[name] = topic
    end

    # Creates a topic with the given attributes
    # @return [Neptune::Topic]
    def create(attrs)
      self << topic = Topic.new(attrs)
      topic
    end

    # Removes the topic, if it exists, with the given name
    # @return [Boolean] true, always
    def delete(name)
      @by_name.delete(name)
      true
    end

    # Adds the given topic, syncing it with any topic that exists with the
    # same name
    # @return [Neptune::Topic]
    def <<(new_topic)
      topic = self[new_topic.name] ||= new_topic
      topic.cluster = @cluster
      topic.attributes = new_topic.attributes unless topic == new_topic
      topic
    end

    # Enumerates over each topic
    def each
      @by_name.each {|name, topic| yield topic}
    end
  end
end