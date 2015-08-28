require 'forwardable'
require 'pp'
require 'neptune/errors'
require 'neptune/error_code'
require 'neptune/support/assertions'
require 'neptune/support/pretty_print'
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

      # The base module for this batch's API
      # @return [Module]
      def api
        @api ||= Api.for_class(self)
      end

      # The name for this batch's API
      # @return [String]
      def api_name
        @api_name ||= Api.name_for(api)
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
      #   # Define a "name" attribute for version 1+
      #   attribute :name, String, version: 1
      #   
      #   # Define a "name" attribute for versions 0 only
      #   attribute :name, String, version: 0..0
      #   
      #   # Define a "name" attribute for versions 0 - 2
      #   attribute :name, String, version: 0..2
      #   
      #   # Define a "brokers" attribute that runs a block after the attribute has been set
      #   attribute :brokers, [Broker] do
      #     @brokers_by_id = ...
      #   end
      # 
      # @!macro [attach] attribute
      #   @!attribute [r] $1
      def attribute(name, type, options = {}, &block)
        assert_valid_keys(options, :version)
        options = {version: 0}.merge(options)

        # Track the definition for usage later
        type = Types::String if type == ::String
        options.merge!(type: type)
        attributes[name] = options

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
      def to_kafka(resource, context = {})
        context[:version] ||= 0
        buffer = Buffer.new

        # Process in reverse order so that the buffer has all the necessary data
        # when the checksum / size is being calculated (if applicable)
        attributes.keys.reverse.each do |attr|
          next unless attribute?(attr, context[:version])

          begin
            value = resource.read_kafka_attribute(attr, context.merge(buffer: buffer))
            buffer.prepend(value)
          rescue => ex
            raise EncodingError.new("[#{name}##{attr}] #{ex.class}: #{ex.message}")
          end
        end

        buffer.to_s
      end

      # Converts from the Kafka data in the current buffer's position
      # @return [Object]
      def from_kafka(buffer, context = {})
        context[:version] ||= 0

        catch(:halt) do
          resource = new

          attributes.each do |attr, *|
            next unless attribute?(attr, context[:version])

            begin
              resource.write_kafka_attribute(attr, buffer, context)
            rescue DecodingError
              # Just re-raise instead of producing a nested exception
              raise
            rescue => ex
              raise DecodingError.new("[#{name}##{attr}] #{ex.class}: #{ex.message}")
            end
          end

          resource
        end
      end

      private
      # Whether the given attribute is applicable within a target API version #
      # @return [Boolean]
      def attribute?(attr, target_version)
        version = attributes[attr][:version]

        case version
        when Fixnum
          version <= target_version
        when Range
          version.include?(target_version)
        else
          raise ArgumentError, "Invalid version for #{attr}: #{version.inspect}"
        end
      end
    end

    include Support::PrettyPrint
    extend Forwardable
    extend Support::Assertions

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
      self.class.attributes.each_with_object({}) do |(attr, *), attributes|
        attributes[attr] = self[attr]
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

    # Reads the value from the given attribute and converts it to Kafka format
    # @return [String]
    def read_kafka_attribute(attr, *args)
      type = self.class.attributes[attr][:type]
      value = self[attr]
      type.to_kafka(value, *args)
    end

    # Writes to the given attribute from a Kafka buffer
    # @private
    def write_kafka_attribute(attr, buffer, context = {})
      type = self.class.attributes[attr][:type]
      value = type.from_kafka(buffer, context)
      self[attr] = value unless value.nil?
    end

    # Converts this class to its Kafka data representation
    # @return [Neptune::Buffer]
    def to_kafka(context = {})
      self.class.to_kafka(self, context)
    end
  end
end