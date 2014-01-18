require 'typhoeus'
require 'json'

module OpenData
  module Connection
    
    def get(options={})
      connect("get", options)
    end

    def post(options={})
      connect("post", options)
    end

    def put(options={})
      connect("put", options)
    end

    def delete(options={})
      connect("delete", options)
    end

    private
    def connect(method="get", options={})
      url = "https://api.mongohq.com/"
      url = "#{url}#{options[:path]}?_apikey=#{@mongohqapikey}"

      method = method.downcase.to_sym
      
      body = options[:body] ||= nil
      body = body.to_json
      
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