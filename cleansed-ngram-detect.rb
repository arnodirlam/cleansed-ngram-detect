# This script detects languages of strings in a database.
# Based on feedbackmine's ruby implementation of the n-gram method.
# 	https://github.com/feedbackmine/language_detector
# 	http://en.wikipedia.org/wiki/N-gram
# 	install using the commands:
#		gem sources -a http://gems.github.com
#		gem install feedbackmine-language_detector
# Designed for big amounts of data.
# Supports saving results in a new instead of the source table.
# Uses activerecord (as used in Ruby on Rails) for general database handling
#	gem install activerecord
# Uses activerecord-import for inserting multiple values into the database efficiently
#	gem install activerecord-import
# Tested with ruby 1.8.7
# Just adjust the values in CONSTANTS and DATABASE CONNECTION and run the script
# 
# I wrote it for a project at university in September 2009.
# Just wanted to share it in case anyone can use it.
# No further development planned so far.

require 'rubygems'
require 'active_record'
# require 'pg'							# uncomment this if you use postgres (gem install pg)
require 'language_detector'
require 'logger'
require 'activerecord-import'

# CONSTANTS
BATCH_LOAD_SIZE = 50000					# number of entries loaded at once from database
BATCH_INSERT_SIZE = 1000				# number of entries inserted at once to database
DB_SOURCE_TABLE = 'langstrings'			# name of the table that holds the strings to be detected
DB_SOURCE_ROW1 = 'id'					# primary key column of the source table
DB_SOURCE_ROW2 = 'str'					# column of the source table that holds the strings to be detected
DB_TARGET_TABLE = 'langstrings-langs'	# name of the table to which the results should be saved
DB_TARGET_ROW1 = 'langstring_id'		# foreign key column of the target table (referencing the source table)
DB_TARGET_ROW2 = 'str_clean'			# column name for the cleansed input strings
DB_TARGET_ROW3 = 'ngram_lang'			# column name for resulting language detected from input string
LOAD_CONDITIONS = nil					# WHERE part for selecting entries from the source table (nil if not applicable)
LOAD_SECURE = false						# true: 	selects only source entries that were not yet inserted in the target table (extremely slow)
										# false: 	orders source by primary key and simply selects next n entries (much faster)
										# use false only if neither your source data or your LOAD_CONDITIONS change during runtime
LOG_FILE_NAME = 'ngram.log'				# file name of the log file (logs all database queries using activerecord's default logger)

unless LOG_FILE_NAME == nil
	logger = Logger.new(LOG_FILE_NAME)
	ActiveRecord::Base.logger = logger
end

# DATABASE CONNECTION
ActiveRecord::Base.establish_connection(
	:adapter  => :mysql,
#	:encoding => :unicode,
	:host     => "localhost",
	:username => "admin",
	:password => "",
	:database => "ngram-test"
)

class SourceTable < ActiveRecord::Base
	set_table_name DB_SOURCE_TABLE
end

class TargetTable < ActiveRecord::Base
	set_table_name DB_TARGET_TABLE
	set_primary_key DB_TARGET_ROW1
end

def format_time (timeElapsed, showSeconds=true)
	#find the seconds
	seconds = timeElapsed % 60

	#find the minutes
	minutes = (timeElapsed / 60) % 60

	#find the hours
	hours = (timeElapsed/3600)

	#format the time

	return hours.to_s + ":" + format("%02d",minutes.to_s) + (showSeconds ? ":" + format("%02d",seconds.to_s) : "") rescue "??:??:??"
end

class EntryServer
	def initialize(logger)
		@logger = logger
		@subquery = TargetTable.select(DB_TARGET_ROW1).to_sql
		@initial_result_count = TargetTable.count
		@detected_count = 0
		puts "counting remaining entries..."
		@remaining_count = SourceTable.count(:conditions => LOAD_CONDITIONS) - @initial_result_count
		puts "#{@remaining_count} entries remaining..."
		@starttime = Time.now
		fetch_next_entries
	end

	def get_entry
		return nil unless remaining_entries?
		fetch_next_entries if @entries.empty?
		@detected_count += 1
		@remaining_count -= 1
		@entries.shift
	end
	
	def remaining_entries?
		@remaining_count > 0
	end

	def remaining_time
		(Time.now - @starttime) / @detected_count * @remaining_count
	end

private
	def fetch_next_entries
		load_count = @remaining_count > BATCH_LOAD_SIZE ? BATCH_LOAD_SIZE : @remaining_count
		select = [DB_SOURCE_ROW1, DB_SOURCE_ROW2].join(', ')
		if LOAD_SECURE
			print "fetching #{load_count} entries... "
			@entries = SourceTable.all(
				:select => select,
				:conditions => [LOAD_CONDITIONS, "#{DB_SOURCE_ROW1} NOT IN (#{@subquery})"].compact.join(' AND '),
				:limit => BATCH_LOAD_SIZE
			)
		else
			offset = @initial_result_count + @detected_count
			print "fetching #{load_count} entries, offset #{offset}... "
			@entries = SourceTable.all(
				:select => select,
				:conditions => LOAD_CONDITIONS,
				:limit => BATCH_LOAD_SIZE,
				:order => DB_SOURCE_ROW1,
				:offset => offset
			)
		end
		puts "done. #{@remaining_count} remaining... #{format_time(remaining_time)} time remaining..."
	end
end

class BulkImporter
	def initialize(model, fieldnames, batchsize)
		@model = model
		@field_names = fieldnames
		@batch_size = batchsize
		@values = []
	end

	def add_values(*args)
		@values << args
		if @values.length >= @batch_size
			self.save!
		end
	end

	def save!
		print "saving #{@values.count} entries to database... "
		@model.import @field_names, @values
		puts "done."
		@values = []
	end
end

s = EntryServer.new(logger)
i = BulkImporter.new(TargetTable, [DB_TARGET_ROW1, DB_TARGET_ROW2, DB_TARGET_ROW3], BATCH_INSERT_SIZE)
d = LanguageDetector.new

while entry = s.get_entry
	str = entry[DB_SOURCE_ROW2]

	# insert cleansing methods for query here
	str = str.gsub(/\d/, '')

	if str == ""
		i.add_values entry[DB_SOURCE_ROW1], str, ''
	else
		begin
			l = d.detect(str)
			i.add_values entry[DB_SOURCE_ROW1], str, l
		rescue
			msg = "ngram detection failed for entry #{[entry[DB_SOURCE_ROW1], entry[DB_SOURCE_ROW2], str].inspect}!"
			puts msg
			logger.error msg
			i.add_values entry[DB_SOURCE_ROW1], str, ''
		end
	end
end

i.save!

puts "FINISHED!"
