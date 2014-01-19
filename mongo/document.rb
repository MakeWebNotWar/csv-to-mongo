module OpenData
  module Mongo
    module Document
      def create_document
        request(@collection_name).insert(@body)
      end

      def get_document
        request(@collection_name).find_one(@id).to_a
      end

      def get_documents
        request(@collection_name).find.to_a
      end

      def update_document

      end

    end
  end
end