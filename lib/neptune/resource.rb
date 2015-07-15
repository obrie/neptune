require 'pp'
require 'neptune/errors'
require 'neptune/types'

module Neptune
  # Represents an object that's been created using content from Kafka. This
  # encapsulates responsibilities for defining schema structure.
  class Resource
    class << self
      def const_missing(name) #:nodoc:
        begin
          Types.const_get(name.to_s)
        rescue NameError
          super
        end
      end

      # Whether the resource can be truncated
      # @return [Boolean]
      def truncatable
        if @truncatable.nil?
          @truncatable = false
        end
        @truncatable
      end
      attr_writer :truncatable

      # The attributes defined for the resource
      # @return [Hash]
      def attributes
        @attributes ||= {}
      end

      # Defines a new Kafka attribute on this class.
      # 
      # @api private
      # @param [String] name The public name for the attribute
      # @param [String] type The data type for the attribute
      # @example
      #   # Define a "name" attribute
      #   attribute :name, String
      #   
      #   # Define a "brokers" attribute that runs a block after the attribute has been set
      #   attribute :brokers, [Broker] do
      #     @brokers_by_id = ...
      #   end
      # 
      # @!macro [attach] attribute
      #   @!attribute [r] $1
      def attribute(name, type, &block)
        # Track the definition for usage later
        attributes[name] = type

        # Reader
        attr_reader(name)

        # Query
        define_method("#{name}?") do
          !!__send__(name)
        end

        # Attribute name conversion
        define_method("#{name}=") do |value|
          instance_variable_set("@#{name}", value)
          instance_eval(&block) if block
        end
      end

      # Converts the given value to its Kafka format
      def to_kafka(resource)
        buffer = Buffer.new

        # Process in reverse order so that the buffer has all the necessary data
        # when the checksum / size is being calculated (if applicable)
        attributes.keys.reverse.each do |attr|
          type = attributes[attr]

          begin
            case attr
            when :checksum
              buffer.prepend(type.to_kafka(buffer.checksum))
            when :size
              buffer.prepend(type.to_kafka(buffer.size))
            else
              value = resource[attr]

              if type.is_a?(Array)
                type = type.first
                value.reverse.each {|v| buffer.prepend(type.to_kafka(v))}
                # FIXME
                buffer.prepend(Types::Int32.to_kafka(value.size)) if !type.respond_to?(:attributes) || !type.attributes.key?(:size)
              else
                buffer.prepend(type.to_kafka(value))
              end
            end
          rescue => ex
            raise EncodingError.new("#{ex.class}: #{ex.message}", ex)
          end
        end

        buffer.to_s
      end

      # Converts from the Kafka data in the current buffer's position
      def from_kafka(buffer)
        resource = new

        # Validate size / checksum
        if attributes.key?(:size)
          # Check if remainder of message is truncated
          if truncatable? && buffer.bytes_remaining >= header_bytes
            resource.truncated = true
            return
          end

          # Determine the data size
          resource.size = attributes[:size].from_kafka(buffer)

          if attributes.key?(:checksum)
            # Determine the valid checksum
            resource.checksum = attributes[:checksum].from_kafka(buffer)
            computed_checksum = [buffer.checksum(resource.size - 4)].pack('l>').unpack('l>').first
            
            resource.checksum_failed = resource.checksum != computed_checksum
            expected_bytes_remaining = resource.size - 4
          else
            expected_bytes_remaining = resource.size
          end

          # Check if remainder of message is truncated
          if truncatable? && expected_bytes_remaining > buffer.bytes_remaining
            resource.truncated = true
            return
          end
        end

        attributes.except(:size, :checksum).each do |attr, type|
          begin
            self[attr] =
              case type
              when Array
                type = type.first
                if !type.respond_to?(:attributes) || !type.attributes.key?(:size)
                  if resource.size
                    array_buffer = Buffer.new(buffer.read(resource.size))
                  else
                    array_buffer = buffer
                  end

                  array = []
                  while !array_buffer.eof? && (v = type.from_kafka(array_buffer))
                    array << v
                  end
                  array
                else
                  Int32.from_kafka(buffer).times.map { type.from_kafka(buffer) }
                end
              else
                type.from_kafka(buffer)
              end
          rescue DecodingError
            # Just re-raise instead of producing a nested exception
            raise
          rescue => ex
            raise DecodingError.new("[Neptune] #{ex.class}: #{ex.message}", ex)
          end
        end
      end

      # The number of bytes expected to be in the header
      # @return [Fixnum]
      def header_bytes
        if attributes.key?(:checksum)
          header_bytes = 8
        else
          header_bytes = 4
        end
      end
    end

    # Whether the resource has been truncated
    # @return [Boolean]
    attr_accessor :truncated

    # Initializes this resources with the given attributes.  This will continue
    # to call the superclass's constructor with any additional arguments that
    # get specified.
    # 
    # @api private
    def initialize(attributes = {}, *args)
      @truncated = false
      self.attributes = attributes
      super(*args)
    end

    # Looks up the value associated with the given attribute
    def [](attr)
      __send__(attr)
    end

    # Sets the attribute to the given value
    def []=(attr, value)
      __send__("#{attr}=", value)
    end

    # Attempts to set attributes on the object only if they've been explicitly
    # defined by the class.
    # 
    # @api private
    # @param [Hash] attributes The updated attributes for the resource
    def attributes=(attributes)
      if attributes
        attributes.each do |attribute, value|
          attribute = attribute.to_s
          __send__("#{attribute}=", value) if respond_to?("#{attribute}=", true)
        end
      end
    end

    # Whether the resource's contents were truncated
    # @return [Boolean]
    def truncated?
      @truncated
    end

    # The name of the error associated with the current error code
    # @return [Symbol]
    def error_name
      ERROR_CODES[error_code]
    end

    # Converts this class to its Kafka data representation
    # @return [Neptune::Buffer]
    def to_kafka
      self.class.to_kafka(self)
    end

    # Forces this object to use PP's implementation of inspection.
    # 
    # @api private
    # @return [String]
    def pretty_print(q)
      q.pp_object(self)
    end
    alias inspect pretty_print_inspect

    # Defines the instance variables that should be printed when inspecting this
    # object.  This ignores the +@cluster+ and +@config+ variables.
    # 
    # @api private
    # @return [Array<Symbol>]
    def pretty_print_instance_variables
      (instance_variables - [:'@cluster', :'@config']).sort
    end
  end
end