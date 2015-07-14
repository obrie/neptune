# Neptune

Neptune is a Kafka client. It's a fork of the [Poseidon](http://github.com/bpot/poseidon) client.

## Usage

```ruby
require 'neptune'

cluster = Neptune::Cluster.new(['localhost:9092'], client_id: 'my_test_producer')
cluster.produce('topic1', 'value1')
```

## Requirements

* Ruby 1.9.3 or higher
* Kafka 0.8 or higher