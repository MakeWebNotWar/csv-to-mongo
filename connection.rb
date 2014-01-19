
require 'typhoeus'
require 'uri'
require 'json'
require "#{File.dirname(__FILE__)}/mongo/request.rb"

module OpenData
  module Connection
    
    def get(options={})
      connect(:get, options)
    end

    def post(options={})
      connect(:post, options)
    end

    def put(options={})
      connect(:put, options)
    end

    def delete(options={})
      connect(:delete, options)
    end

    private

    def connect(method, options={})
      adapter = options[:adapter] ||= :mongo
      adapter = adapter.downcase.to_sym

      if adapter === :rest
        rest_connect(method, options)
      else
        mongo_connect(method, options)
      end
    end

    def mongo_connect(method=:get, options={})
      path = options[:path]
      document = options[:body] ||= nil
      
      options = {
        method: method,
        document: document
      }

      request = OpenData::Mongo::Request.new(path, options)
      request.run
    end

    def rest_connect(method=:get, options={})
      apikey = ENV['MONGO_APIKEY']

      url = "https://api.mongohq.com/"
      url = "#{url}#{options[:path]}?_apikey=#{apikey}"
     
      if options[:body]
        body = {}
        body[:document] = options[:body]
        body = body.to_json
      else
        body = nil
      end
     
      params = {}
      if options[:params]
        params.merge!(options[:params])
      end
      
      headers = {
        :"Content-Type" => "application/json",
        :Accept => "application/json"
      }
      if options[:headers]
        headers.merge!(options[:headers])
      end

      request = Typhoeus::Request.new(
        url,
        method: method,
        body: body,
        params: params,
        headers: headers
      )
      request.run
    end

  end
end