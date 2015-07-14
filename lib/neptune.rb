require 'logger'

# Kafka API for Ruby
module Neptune
  autoload :Cluster, 'neptune/cluster'

  class << self
    # The logger to use for all Spotify messages.  By default, everything is
    # logged to STDOUT.
    # @return [Logger]
    attr_accessor :logger
  end

  @logger = Logger.new(STDOUT)
end