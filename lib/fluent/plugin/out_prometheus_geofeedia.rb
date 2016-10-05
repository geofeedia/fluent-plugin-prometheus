require 'fluent/output'
require 'fluent/plugin/prometheus'

module Fluent
  class PrometheusGeofeediaOutput < Output
    Plugin.register_output('prometheus_geofeedia', self)
    include Fluent::Prometheus

    # in milliseconds
    DEFAULT_BUCKETS = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000].freeze

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
            if /(count|error|success)<(int|long)>$/.match key
              key_sym = key_symbol(key, record)
              counter = @registry.exist?(key_sym) ? @registry.get(key_sym) : @registry.counter(key_sym, 'counter')
              counter.increment(labels, value)

            elsif /(duration|size|took)<(int|long)>$/.match key
              key_sym = key_symbol(key, record)
              histogram = @registry.exist?(key_sym) ? @registry.get(key_sym) : @registry.histogram(key_sym, 'histogram', {}, DEFAULT_BUCKETS)
              histogram.observe(labels, value)
              
            end

          end

        rescue => e
          $log.error("Prometheus Geofeedia error:", :error_class => e.class, :error => e.message)
          # $log.error(e.backtrace)
        end
      end

      chain.next
    end
  end
end

def standard_labels(record)
  labels = {}

  labels[:service] = record['service'] if record.has_key? 'service'
  labels[:release] = record['release'] if record.has_key? 'release'

  record.each do |key, value|
    if key.start_with? 'placement.'
      labels[key.gsub(/\./, '_').to_sym] = value
    end
  end

  return labels
end

def key_symbol(key, record)
  key_base = key.gsub(/<(\w+)>$/, '').gsub(/\./, '_') # remove the type identifier suffix and replace periods

  if record.has_key? 'schema' && record['schema'] == 'woodpecker.v1'
    # combine module_submodule_action_key
    return ([ record['module'], record['submodule'], record['action'], key_base ] * '_').to_sym
  end

  return key_base.to_sym
end
