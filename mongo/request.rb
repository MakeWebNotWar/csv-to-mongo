require 'mongo'

module OpenData
  module Mongo
    class Request
      
      include ::Mongo

      def initialize(path, options={})
        @path = path.split("/")

        @database     = ENV['MONGO_DATABASE']
        @collection   = @path[1]
        @document     = @path[3]

        @method   = options[:method]
        @body = options[:document]

        @url      = options[:url]       ||= ENV['MONGO_URL']
        @port     = options[:port]      ||= ENV['MONGO_PORT']
        @username = options[:username]  ||= ENV['MONGO_USERNAME']
        @password = options[:password]  ||= ENV['MONGO_PASSWORD']
      end

      def run
        request = get_connection

        case @method
        when :post
          if has_collections?
            if @collection && is_document? && @document.nil?
              response = request.collection("#{@collection}").insert(@body)
            end
          end
        end

        request.connection.close
        @document = response
      end

      private

      def get_connection
        request = ::Mongo::Connection.new(@url, @port).db(@database)
        request.authenticate(@username, @password) unless (@username.nil? || @password.nil?)
        request
      end

      def has_collections?
        @path[0] === "collections" ? true : false
      end

      def is_document?
        @path[2] === "documents" ? true : false
      end

    end
  end
end