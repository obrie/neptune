$LOAD_PATH.unshift File.expand_path('../lib', __FILE__)
require 'neptune/version'

Gem::Specification.new do |s|
  s.name              = "neptune"
  s.version           = Neptune::Version::STRING
  s.authors           = ["Aaron Pfeifer"]
  s.email             = "aaron.pfeifer@gmail.com"
  s.homepage          = "http://github.com/obrie/neptune"
  s.description       = "Kafka Producer API for Ruby"
  s.summary           = "Kafka Producer API for Ruby"
  s.require_paths     = ["lib"]
  s.files             = `git ls-files`.split("\n")
  s.test_files        = `git ls-files -- test/*`.split("\n")
  s.rdoc_options      = %w(--line-numbers --inline-source --title neptune --main README.md)
  s.extra_rdoc_files  = %w(README.md CHANGELOG.md LICENSE)

  s.add_development_dependency("rake")
  s.add_development_dependency("rspec", "~> 3.0")
  s.add_development_dependency("simplecov")
end