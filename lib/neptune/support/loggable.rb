module Neptune
  module Support
    # Provides a set of helper methods for logging
    # @api private
    module Loggable
      private
      # Delegates access to the logger to Neptune.logger
      # @return [Logger]
      def logger
        Neptune.logger
      end
    end
  end
end