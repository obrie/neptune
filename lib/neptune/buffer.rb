module Neptune
  # Represents Kafka-encoded data
  class Buffer
    def initialize(data = '') #:nodoc:
      @data = data.encode(Encoding::BINARY)
      @pos = 0
    end

    # Rewinds the position back to the start of the buffer
    # @return [Neptune::Buffer]
    def rewind
      @pos = 0
      self
    end

    # Adds the given value to the end of the buffer
    # @return [Neptune::Buffer]
    def concat(value)
      @data.concat(value.dup.force_encoding(Encoding::BINARY))
      self
    end

    # Adds the given value to the beginning of the buffer
    # @return [Neptune::Buffer]
    def prepend(value)
      @data.prepend(value.dup.force_encoding(Encoding::BINARY))
      self
    end

    # The current length of the buffer
    # @return [Fixnum]
    def size
      @data.bytesize
    end

    # Retrieves the given number of bytes, moving the current position
    # @return [String]
    def read(length = bytes_remaining)
      value = @data.byteslice(@pos, length)
      @pos += length
      value
    end

    # Retrieves the given number of bytes without moving the current position
    # @return [String]
    def peek(length)
      @data.byteslice(@pos, length)
    end

    # The number of bytes remaining in the buffer
    # @return [Fixnum]
    def bytes_remaining
      @data.bytesize - @pos
    end

    # Determines whether we've reached the end of the buffer
    # @return [Boolean]
    def eof?
      @pos == @data.bytesize
    end

    # Generates a string representation of this buffer
    # @return [String]
    def to_s
      @data
    end
  end
end