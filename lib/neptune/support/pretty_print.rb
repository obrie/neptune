module Neptune
  module Support
    # Provides a standard interface for inspecting objects
    module PrettyPrint
      # Forces this object to use PP's implementation of inspection.
      #
      # @api private
      # @return [String]
      def pretty_print(q)
        q.pp_object(self)
      end

      def inspect #:nodoc:
        pretty_print_inspect
      end

      # Defines the instance variables that should be printed when inspecting this
      # object.
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
end