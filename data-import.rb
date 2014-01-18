require 'csv'
require 'uri'
require 'mongo'
require 'bson'
require "#{File.dirname(__FILE__)}/object.rb"

module OpenData
  class Importer
    
    attr_reader :source,
                :filetype,
                :raw_data,
                :attributes,
                :items

    def initialize(file, options={})
      if file
        @database = options[:database]
        @collection = options[:collection]
        @source = file
        @filetype = File.extname(@source)
        @raw_data = nil
        @items = nil
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
      items = []

      case @filetype
      when ".csv"
        @raw_data.each do |row|
          item = {}
          row.each_with_index do |value, index|
            item[:"#{@attributes[index]}"] = value
          end
          items.push(item)
        end

      when ".json"
        item = JSON.parse(@raw_data)
        item = keys_to_symbol(item)
        items.push(item)

      end

      @items = items
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
    
    def create_objects
      length = @items.size
      done = 0
      @items.each do |object|
        if create_object(object)
          done = done+1
          puts "ok"
        end

        percent = (done/length)*100

        puts "#{percent}% done"

      end
    end

    def create_object(object)
      document = Object.new(object)
      document.save
    end

  end
end