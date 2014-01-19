require "#{File.dirname(__FILE__)}/connection.rb"

module OpenData
  class Document
    
    include OpenData::Connection

    def initialize(document, collection, options={})
      @database = options[:database] ||= nil
      @collection = collection
      @path = generate_path
      build_document(document)
    end

    def save
      document = build_attributes

      options = {
        path: @path,
        body: document
      }

      request = post(options)
    end
    
    private
    
    def build_document(document)
      if document.is_a? Hash
        document.each do |attribute, value|
          self.class.send(:attr_accessor, attribute)
          instance_variable_set("@#{attribute}", value)
        end
      else
        raise "Requires a Hash"
      end
    end

    def build_attributes
      exclude_attribute = %w[database collection path]
      attributes = {}
      
      instance_variables.each do |variable|
        name = variable.to_s
        name = name[1..name.size]
        value = instance_variable_get variable
        attributes[name] = value     
      end

      exclude_attribute.each do |attribute|
        attributes.delete(attribute)
      end

      attributes
    end

    def generate_path
      if @database
        "databases/#{@database}/collections/#{@collection}/documents"
      else
        "collections/#{@collection}/documents"
      end
    end

  end
end