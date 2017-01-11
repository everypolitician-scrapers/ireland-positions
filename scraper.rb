#!/bin/env ruby
# encoding: utf-8
# frozen_string_literal: true

require 'everypolitician'
require 'scraperwiki'

require 'pry'

module Wikisnakker
  class Item
    QUALIFIERS = {
      P102:  :party,
      P155:  :follows,
      P156:  :followed_by,
      P580:  :start_date,
      P582:  :end_date,
      P642:  :of,
      P768:  :constituency,
      P1365: :replaces,
      P1366: :replaced_by,
      P2937: :term,
    }.freeze

    def positions
      return [] if self.P39s.empty?
      self.P39s.map do |posn|
        quals = posn.qualifiers
        qdata = quals.properties.partition { |p| QUALIFIERS[p] }
        qgood = qdata.first.map { |p| [QUALIFIERS[p], quals[p].value.to_s] }.to_h
        warn "#{id}: #{posn.value.id} + unknown #{qdata.last.join(', ')}" unless qdata.last.empty?

        {
          id:          id,
          position:    posn.value.id,
          label:       posn.value.to_s,
          description: posn.value.description(:en).to_s,
          start_date:  '' # need _something_ here so we can key on it
        }.merge(qgood) rescue {}
      end
    end
  end
end

module Wikidata
  require 'wikisnakker'

  class Positions
    def initialize(ids:)
      @ids = ids
    end

    def positions
      wikisnakker_items.flat_map(&:positions).compact
    end

    private

    attr_reader :ids

    def wikisnakker_items
      @wsitems ||= Wikisnakker::Item.find(ids)
    end
  end
end

house = EveryPolitician::Index.new.country('Ireland').lower_house
wanted = house.popolo.persons.map(&:wikidata).compact
data = Wikidata::Positions.new(ids: wanted).positions
ScraperWiki.save_sqlite(%i(id position start_date), data)
