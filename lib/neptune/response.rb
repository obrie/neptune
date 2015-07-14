require 'neptune/resource'

module Neptune
  class Response < Resource
    # @return [Fixnum]
    attribute :size, Int32

    # @return [Fixnum]
    attribute :correlation_id, Int32
  end
end