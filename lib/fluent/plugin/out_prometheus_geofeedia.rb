require 'fluent/output'
require 'fluent/plugin/prometheus'

module Fluent
  class PrometheusGeofeediaOutput < Output
    Plugin.register_output('prometheus_geofeedia', self)
    include Fluent::Prometheus

    def initialize
      super
      @registry = ::Prometheus::Client.registry
    end

    def configure(conf)
      super
    end

    def emit(tag, es, chain)
      es.each do |time, record|
        begin
          labels = standard_labels(record)

          record.each do |key, value|

            # Look for specific keys that we know how to map to metrics
            if /(count|error|size|success)<(int|long)>$/.match key
              key_sym = key_symbol(key, record)
              counter = @registry.exist?(key_sym) ? @registry.get(key_sym) : @registry.counter(key_sym, 'counter')
              counter.increment(labels, value.to_i)
            elsif /(duration|took)<(int|long)>$/.match key
              key_sym = key_symbol(key, record)
              summary = @registry.exist?(key_sym) ? @registry.get(key_sym) : @registry.summary(key_sym, 'summary')
              summary.observe(labels, value.to_i)
            end

          end

        rescue => e
          $log.error("Prometheus Geofeedia error:", :error_class => e.class, :error => e.message, :record => record)
          # $log.error(e.backtrace)
        end
      end

      chain.next
    end
  end
end

def standard_labels(record)
  labels = {}

  attrs = %w(service release placement.cloud placement.env placement.hostname 
    placement.instanceid placement.podname placement.region placement.zone)

  attrs.each do |key|
    labels[key.to_sym] = record.has_key?(key) ? record[key] : nil
  end

  return labels
end

def key_symbol(key, record)
  key_base = key.gsub(/<(\w+)>$/, '').gsub(/\./, '_') # remove the type identifier suffix and replace periods

  if record.has_key?('schema') && record['schema'] == 'woodpecker.v1'
    # combine module_submodule_action_key
    return ([ record['module'], record['submodule'], record['action'], key_base ] * '_').to_sym
  end

  return key_base.to_sym
end
