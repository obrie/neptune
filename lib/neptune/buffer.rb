module Neptune
  # Represents binary strings
  class Buffer
    def initialize(str = ''.encode(Encoding::BINARY)) #:nodoc:
      @str = str
      @pos = 0
    end

    # Adds the given value to the end of the buffer
    # @return [Neptune::Buffer]
    def concat(value)
      @str.concat(value.dup.force_encoding(Encoding::BINARY))
      self
    end

    # Adds the given value to the beginning of the buffer
    # @return [Neptune::Buffer]
    def prepend(value)
      @str.prepend(value.dup.force_encoding(Encoding::BINARY))
      self
    end

    # The current length of the buffer
    # @return [Fixnum]
    def size
      @str.bytesize
    end

    # Retrieves the given number of bytes, moving the current position
    # @return [String]
    def read(length)
      value = @str.byteslice(@pos, length)
      @pos += length
      value
    end

    # Retrieves the given number of bytes without moving the current position
    # @return [String]
    def peek(length)
      @str.byteslice(@pos, length)
    end

    # The number of bytes remaining in the buffer
    # @return [Fixnum]
    def bytes_remaining
      @str.bytesize - @pos
    end

    # Determines whether we've reached the end of the buffer
    # @return [Boolean]
    def eof?
      @pos == @str.bytesize
    end

    # Generates a string representation of this buffer
    # @return [String]
    def to_s
      @str
    end
  end
end