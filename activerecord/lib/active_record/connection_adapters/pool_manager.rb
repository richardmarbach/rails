# frozen_string_literal: true

require "concurrent/atomic/read_write_lock"

module ActiveRecord
  module ConnectionAdapters
    class PoolManager # :nodoc:
      def initialize
        @role_to_shard_mapping = Hash.new { |h, k| h[k] = {} }
        @lock = Concurrent::ReadWriteLock.new
      end

      def shard_names
        @lock.with_read_lock do
          @role_to_shard_mapping.values.flat_map { |shard_map| shard_map.keys }.uniq
        end
      end

      def role_names
        @lock.with_read_lock do
          @role_to_shard_mapping.keys
        end
      end

      def pool_configs(role = nil)
        @lock.with_read_lock do
          if role
            @role_to_shard_mapping[role].values
          else
            @role_to_shard_mapping.flat_map { |_, shard_map| shard_map.values }
          end
        end
      end

      def each_pool_config(role = nil, &block)
        @lock.with_read_lock do
          if role
            @role_to_shard_mapping[role].each_value(&block)
          else
            @role_to_shard_mapping.each_value do |shard_map|
              shard_map.each_value(&block)
            end
          end
        end
      end

      def remove_role(role)
        @lock.with_read_lock do
          @role_to_shard_mapping.delete(role)
        end
      end

      def remove_pool_config(role, shard)
        @lock.with_read_lock do
          @role_to_shard_mapping[role].delete(shard)
        end
      end

      def get_pool_config(role, shard)
        @lock.with_read_lock do
          @role_to_shard_mapping[role][shard]
        end
      end

      def set_pool_config(role, shard, pool_config)
        if pool_config
          @lock.with_write_lock do
            @role_to_shard_mapping[role][shard] = pool_config
          end
        else
          raise ArgumentError, "The `pool_config` for the :#{role} role and :#{shard} shard was `nil`. Please check your configuration. If you want your writing role to be something other than `:writing` set `config.active_record.writing_role` in your application configuration. The same setting should be applied for the `reading_role` if applicable."
        end
      end
    end
  end
end
