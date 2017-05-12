#!/bin/env ruby
# encoding: utf-8
# frozen_string_literal: true

require 'everypolitician'
require 'pry'
require 'scraperwiki'

require_relative 'lib/politician'

ScraperWiki.sqliteexecute('DROP TABLE data') rescue nil
house = EveryPolitician::Index.new.country('Ireland').lower_house
wanted = house.popolo.persons.map(&:wikidata).compact
data = Wikisnakker::Politician.find(wanted).flat_map(&:positions).compact
ScraperWiki.save_sqlite(%i(id position start_date), data)
