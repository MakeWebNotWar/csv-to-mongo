require "#{File.dirname(__FILE__)}/connection.rb"

module OpenData
  class Object
    
    include OpenData::Connection

    PATH = "databases/odata/collections/marriages/documents"

    def initialize(object, options={})
      # build_object(object)
      build_object(object)
      
      if options[:apikey]
        @mongohqapikey = options[:apikey]
      else
        @mongohqapikey = ENV['MONGOHQ_APIKEY'] ||= nil
      end
    end

    def build_object(object)
      if object.is_a? Hash
        object.each do |attribute, value|
          self.class.send(:attr_accessor, attribute)
          instance_variable_set("@#{attribute}", value)
        end
      else
        raise "Requires a Hash"
      end
    end

    def build_attributes
      attributes = {}
      instance_variables.each do |variable|
        name = variable.to_s
        name = name[1..name.size]
        if name != "mongohqapikey"
          value = instance_variable_get variable
          attributes[name] = value
        end
      end
      attributes
    end

    def save
      
      path = @id ? "#{PATH}/#{@id}" : "#{PATH}" 
      
      document = {}
      # document[:safe] = true
      document[:document] = build_attributes

      options = {
        path: path,
        body: document
      }

      request = post(options)

      # if request.success?
      #   true
      # else
      #   false
      # end
    end

  end
end