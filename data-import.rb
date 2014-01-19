require 'csv'
require 'uri'
require 'mongo'
require 'bson'
require 'ruby-progressbar'
require "#{File.dirname(__FILE__)}/document.rb"

module OpenData
  class Importer
    
    attr_reader :source,
                :filetype,
                :raw_data,
                :attributes,
                :documents

    def initialize(file, collection, options={})
      if file
        @database = options[:database]
        @collection = collection
        @source = file
        @filetype = File.extname(@source)
        @raw_data = nil
        @documents = nil
        get_data
      else
        raise "Need to specify a file or url for data source"
      end
    end

    def get_data
      if @filetype === '.csv'
        @raw_data = CSV.read(@source, {:encoding => "windows-1251:utf-8", :headers => false})
        define_attributes
      elsif @filetype === '.json'
        @raw_data = File.read(@source)
      end
      map_to_hash
    end

    def define_attributes
      @attributes = @raw_data.shift     
    end

    def map_to_hash
      documents = []

      case @filetype
      when ".csv"
        @raw_data.each do |row|
          item = {}
          row.each_with_index do |value, index|
            item[:"#{@attributes[index]}"] = value
          end
          documents.push(item)
        end

      when ".json"
        item = JSON.parse(@raw_data)
        item = keys_to_symbol(item)
        documents.push(item)

      end

      @documents = documents
    end

    def keys_to_symbol(old_hash)
      new_hash = {}
      if old_hash.is_a? Hash
        old_hash.each do |key, value|
          if value.is_a? Hash
            value = keys_to_symbol(value)
          end
          key = key.downcase.to_sym
          new_hash[key] = value
        end
        return new_hash
      else
        raise "Must be a hash"
      end
    end
    
    def create_documents

      # $stdout.flush
      # $stdout.write "\rFile: #{file} of #{@documents.size}"
      file = 0
      progressbar = ProgressBar.create(
        :title => "Uploaded", 
        :total => @documents.size, 
        :format => '%a %E |%b>>%i| %c of %C / %p%% %t')
      system "clear" unless system "cls"
      @documents.each do |document|
        if create_document(document)

          progressbar.increment
        else
          return "Error"
        end
      end
    end

    def create_document(document)
      document = Document.new(document, @collection, {database: @database})
      document.save
    end

  end
end