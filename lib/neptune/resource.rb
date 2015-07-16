require 'pp'
require 'neptune/errors'
require 'neptune/error_code'
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

      # The attributes defined for the resource
      # @return [Hash]
      def attributes
        @attributes ||= superclass.respond_to?(:attributes) ? superclass.attributes.dup : {}
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
        type = Types::String if type == ::String
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
      # @return [String]
      def to_kafka(resource)
        buffer = Buffer.new

        # Process in reverse order so that the buffer has all the necessary data
        # when the checksum / size is being calculated (if applicable)
        attributes.keys.reverse.each do |attr|
          type = attributes[attr]
          value = resource[attr]

          begin
            buffer.prepend(type.to_kafka(value, buffer))
          rescue => ex
            raise EncodingError.new("#{ex.class}: #{ex.message}", ex)
          end
        end

        buffer.to_s
      end

      # Converts from the Kafka data in the current buffer's position
      # @return [Object]
      def from_kafka(buffer)
        catch(:halt) do
          resource = new

          attributes.each do |attr, type|
            begin
              value = type.from_kafka(buffer)
              resource[attr] = value unless value.nil?
            rescue DecodingError
              # Just re-raise instead of producing a nested exception
              raise
            rescue => ex
              raise DecodingError.new("#{ex.class}: #{ex.message}", ex)
            end
          end

          resource
        end
      end
    end

    # Initializes this resources with the given attributes.  This will continue
    # to call the superclass's constructor with any additional arguments that
    # get specified.
    # 
    # @api private
    def initialize(attributes = {}, *args)
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

    # The attributes defined for this resource
    # @return [Hash]
    def attributes
      self.class.attributes.keys.inject({}) do |attributes, attr|
        attributes[attr] = self[attr]
        attributes
      end
    end

    # Attempts to set attributes on the object only if they've been explicitly
    # defined by the class.
    # 
    # @param [Hash] attributes The updated attributes for the resource
    def attributes=(attributes)
      if attributes
        attributes.each do |attr, value|
          self[attr] = value
        end
      end
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
      (instance_variables - pretty_print_ignore).sort
    end

    private
    def pretty_print_ignore #:nodoc:
      []
    end
  end
end