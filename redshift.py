#!/usr/bin/python
# -*- coding: utf8 -*-

#Useful module for grabbing data from Amazon Redshift and manipulating it
# - Most functions either have a docstring or are fairly self explanatory
# - There's another module needed called login but that contains a password
#	so isn't listed here.
# - Many functions require a cache to be set up beforehand

from collections import defaultdict
from os import path, remove
import json, cPickle
from datetime import datetime, timedelta

import psycopg2
from login import login_string #login details, not committed

verbose = False #debug setting

############# Basic caching and setup ##################

def query_database(query, show_query=True):
	"""Creates a cursor, makes a query, closes the cursor, returns the data.
	This queries the main impression_stats_daily redshift database"""
	
	conn = psycopg2.connect(login_string) #make cursor
	cursor = conn.cursor()
	if show_query:
		print query
	cursor.execute(query) #query db
	data = cursor.fetchall() #get data
	cursor.close() #close
	
	return data

def query_new_database(query, show_query=True):
	"""Creates a cursor, makes a query, closes the cursor, returns the data.
	This is with the second tiles database"""
	
	new_login_string = login_string.replace('tiles-prod-redshift.prod.mozaws.net', 'rds.tiles.prod.mozaws.net')	
	conn = psycopg2.connect(new_login_string) #make cursor
	cursor = conn.cursor()
	if show_query:
		print query
	cursor.execute(query) #query
	data = {
		'data': cursor.fetchall(),
		'description': [desc[0] for desc in cursor.description]
	}
	cursor.close() #close
	
	return data

def build_tiles_cache(force_redownload=False):
	"""Builds a tiles cache. This is a list of lists"""
	
	cache_file_location = "tiles.cache"
	
	#Work out if the cache needs to be rebuilt
	redownload = False
	exists = False
	
	if force_redownload:
		redownload = True
		exists = True
	else:
		if not path.isfile(cache_file_location): #check if it exists
			redownload = True
		else:
			exists = True
			#get file last modified date
			last_modified = datetime.fromtimestamp(path.getmtime(cache_file_location))
			now = datetime.now()
			if (datetime.now()-last_modified).days > 0:
				redownload = True
	
	if redownload:
		if exists:
			remove(cache_file_location) #delete the current file
		
		#get the tile data from the server
		print "Refreshing tiles cache from remote server (will take ~2 seconds)...",
		tile_data = query_new_database("SELECT * FROM tiles;", show_query=False)
		print "done"
		
		#now we have to build the tiles database
		#according to campaign rules etc
		cache = {}
		cache['tiles'] = build_tiles_database(tile_data)
		cache['countries'] = get_all_countries_from_server()
		
		#save cache
		with open(cache_file_location, 'w') as f:
			cPickle.dump(cache, f)
		
	else:
		
		#just load the cache from the local folder
		with open(cache_file_location) as f:
			try:
				cache = cPickle.load(f)
			except Exception:
				remove('tiles.cache') #best I can do
				print "Zenko updated to version 2.0! Please restart to see the new interface." 
				exit()
	
	return cache

def build_reverse_matchers():
	"""Imports the campaign mapping and builds a reverse matcher payload"""

	with open('campaign_mapping.json') as f:
		campaign_mapping = json.load(f)

	#make reverse mapping of matchers
	reverse_matchers = defaultdict(dict)
	for client, campaign_info in campaign_mapping.iteritems():
		for campaign, matcher_info in campaign_info.iteritems():
			for matcher_type, matchers in matcher_info.iteritems():
				for matcher in matchers:
					matcher_type = matcher_type.replace('_must_match','')
					reverse_matchers[matcher_type][matcher] = [client, campaign]
	
	return reverse_matchers

def get_client_statuses():
	"""Gets the payload of clients that are archived or current, and produces a mapping"""
	
	with open('client_status.json') as f:
		statuses = json.load(f)
	
	mapping = {}
	for status, clients in statuses.iteritems():
		for client in clients:
			mapping[client] = status
	
	return mapping

def build_tiles_database(data):
	"""Builds a tiles database from scratch in the local folder"""
	
	#we have a list of lists, this needs to be a list of dictionaries
	#we need to iterate through each element (a sublist), turn it into a dictionary
	#and add attributes
	
	#build the campaign/client entries in the cache
	reverse_matchers = build_reverse_matchers()
	statuses = get_client_statuses()
	
	#turn each list element into a dictionary
	#and give each one a client and campaign
	for tile_index in range(len(data['data'])):
		
		new_entry = {}
		for n, column_value in enumerate(data['data'][tile_index]):
			
			column_name = data['description'][n]
			if type(column_value) == str:
				column_value = column_value.decode('utf8')
			
			new_entry[column_name] = column_value
			
			if 'client' not in new_entry:
				if column_name in reverse_matchers:
					rm = reverse_matchers[column_name] #for brevity
					match = False
					if type(column_value) == int:
						if column_value in rm:
							match = rm[column_value]
					else:
						if column_value in rm:
							match = rm[column_value]
						else:
							for x in rm:
								if x.startswith('='): #exact match
									if x[1:] == column_value:
										match = rm[x]
										break
								else:
									if x in column_value:
										match = rm[x]
										break
					if match:
						new_entry['client'], new_entry['campaign'] = match
		
		if 'client' not in new_entry:
			print u"Uncategorized! {0} - {1} - {2}".format(new_entry['id'], new_entry['title'], new_entry['target_url'])
			new_entry['client'] = new_entry['title']
			new_entry['campaign'] = False
			#continue
		
		if new_entry['client'] in statuses:
			new_entry['status'] = statuses[new_entry['client']]
		else:
			new_entry['status'] = "current" #default to current tile
		
		data['data'][tile_index] = new_entry
	
	return data['data']

def get_all_countries_from_server():
	"""Gets a list of all countries in the database"""
	
	print "Refreshing a list of all countries from remote server (will take ~1 second)...",
	
	query = "SELECT * FROM countries"
	data = query_database(query, show_query=False)

	countries = {
		"code_to_country": {x[1]:x[0] for x in data},
		"country_to_code": {x[0]:x[1] for x in data}
	}
	
	print "done"
	
	return countries

######### Querying client/tile data ###########

def get_meta_data(cache, selectors):
	"""Calculates meta-data for a selection
	e.g. locales, locale-count
	"""
	
	meta_data = defaultdict(set)
	
	#if tile is relevant, add info to meta_data
	for tile in cache['tiles']:
		add = True
		for k, v in selectors.iteritems():
			if k in tile:
				if tile[k] not in v:
					add = False
					break
		if add:
			for k, v in tile.iteritems():
				meta_data[k].update([v])
	
	#work out counts
	for k in meta_data.keys():
		meta_data[k + "_count"] = len(meta_data[k])
	
	#work out start_date
	meta_data['created_at'] = min(meta_data['created_at'])
	
	return meta_data

def filter_yahoo_tiles(tiles):
	"""Filters yahoo tiles from an existing set of tiles pulled from the cache.
	Returns a dictionary with two lists of lists.
	Each element in the list is [category/flight, csv of tile ids]
	"""
	
	#category repository
	cats = defaultdict(list)
	
	#also have to provide week information
	#have to wind things up to each next monday
	flight_start_dates = defaultdict(list)
	jumps = {
				'Monday': 0,
				'Tuesday': 6,
				'Wednesday': 5,
				'Thursday': 4,
				'Friday': 3,
				'Saturday': 2,
				'Sunday': 1
			}
	
	for tile in tiles:
		if 'mz_cat' in tile['target_url']:
			cat = tile['target_url'].split('mz_cat=')[1].split('&')[0].strip()
			cats[cat].append(tile['id'])
		else:
			#utm params can be missing
			sub = tile['target_url'].split('yahoo.com/')[1].split('/')[0].strip()
			if sub == 'travel':
				cats['travel_gen'].append(tile['id'])
		
		date = datetime.strptime(tile['created_at'].split()[0], '%Y-%m-%d')
		day = date.strftime('%A')
		
		#find the next Monday
		next_monday = (date + timedelta(days=jumps[day])).strftime('Week starting Monday %Y-%m-%d')
		flight_start_dates[next_monday].append(tile['id'])
		
	cats = [[k, ','.join(v)] for k,v in cats.iteritems()]
	cats = sorted(cats)
	flight_start_dates = [[k, ','.join(v)] for k,v in flight_start_dates.iteritems()]
	flight_start_dates = sorted(flight_start_dates)
	
	yahoo_filter = {
		'flight_start_dates': flight_start_dates,
		'categories': cats
	}
	
	return yahoo_filter

def get_tile_data(cache, selectors={}, attributes=False, sort=False):
	"""Gets tile info according to selectors.
	If attributes is specified, it only gets data of those attributes, and dedupes
	If sort is specified, it sorts by a key"""
	
	items = []
	
	if type(attributes) == list:
		attributes = set(attributes)
	else:
		attributes = set([attributes])

	for tile in cache['tiles']:
		add = True
		for k, v in selectors.iteritems():
			if k in tile:
				if tile[k] not in v:
					add = False
					break
		if add:
			tmpTile = {}
			if attributes:
				for k, v in tile.iteritems():
					if k in attributes:
						tmpTile[k] = v
			if tmpTile:
				items.append(tmpTile)
			else:
				items.append(tile)
	
	#now have to dedupe
	if attributes:
		items = [dict(t) for t in set([tuple(d.items()) for d in items])]
		if sort:
			items = sorted(items, key=lambda x: x[sort])
	
	return items

########## Engagement ###########

def engagement_grade(engagement):
	"""Scores the engagement with a letter"""
	
	over = [
		[997, "A++"],
		[992, "A+"],
		[979, "A"],
		[971, "B++"],
		[960, "B+"],
		[951, "B"],
		[933, "C++"],
		[898, "C+"],
		[0, "C"]
	]
	
	for score in over:
		if engagement >= score[0]:
			return score[1]
	
	return "?"

def engagement(blocks, clicks):
	"""Adds m7 engagement"""

	if clicks == 0:
		return 0
	
	e = float(blocks) / clicks
	#e = e * (blocks * clicks)
	e = e * 10
	e = 1000 - e
	e = int(e)
	
	return e

########## Querying impressions data ###########

def get_overview_impressions_data(cache):
	"""Special case, pivot by client and campaign. Result should be like:
	
	client	campaign	imps	clicks	pins	blocks
	mozilla	x
	
	temp fix
	
	"""
	
	
	
	tile_groups = defaultdict(set)
	
	for x in cache['tiles']:
		if x['campaign']:
			col_name = x['client'] + " (" + x['campaign'] + ")"
		else:
			col_name = x['client'] + " (all)"
		
		tile_groups[col_name].update([x['id']])
	
	from codecs import open as copen
	
	with copen('testquery', 'w', encoding='utf8') as f:
		for gname, group in tile_groups.iteritems():
			query = "SELECT"
			sname = gname.replace(' ', "_")
			sname = sname.replace('.', "_")
			sname = sname.replace('(', "_")
			sname = sname.replace(')', "_")
			sname = sname.replace('#', "_")
			sname = sname.replace('!', "_")
			sname = sname.replace(u'ô', "o")
			sname = sname.replace(u'é', "e")
			group = ", ".join([str(x) for x in group])
			for col in ['impressions', 'clicks', 'pinned', 'blocked']:
				query += u"\nsum({0}) as {0},".format(col)
			query = query[:-1]
		
			query += "\nFROM impression_stats_daily"
			query += "\nWHERE date >= 2015-09-01"
			query += "\n AND tile_id in ({0});".format(group)
			print gname, sname
			data = query_database(query)
			
			line = gname + "\t" + u"\t".join([unicode(x) for x in data[0]])
			
			f.write(line + u"\n")

def get_impressions_data(cache, pivot, selectors):
	"""Makes a sql query and gets impressions data"""
	
	#find tiles according to selectors
	tile_ids = get_tile_data(cache, selectors, attributes='id')
	tile_ids = ", ".join([str(x['id']) for x in tile_ids])
	
	#optionally add country to where clause
	countries = ""
	if pivot == 'country_name':
		pivot = 'country_code'
	
	if 'country_name' in selectors:
		countries = ["'{0}'".format(cache['countries']['country_to_code'][x]) for x in selectors['country_name']]
		countries = "AND country_code in ({0})".format(", ".join(countries))
	
	#optionally add in dates
	dates = ""
	if ('start_date' in selectors) or ('end_date' in selectors):
		dates = []
		if 'start_date' in selectors:
			dates.append("DATE >= '" + list(selectors['start_date'])[0] + "'")
		if 'end_date' in selectors:
			dates.append("DATE <= '" + list(selectors['end_date'])[0] + "'")
		dates = "AND " + " AND ".join(dates)
	
	query = """
	SELECT {0}, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
	FROM impression_stats_daily
	WHERE tile_id in ({1})
	{2}
	{3}
	GROUP BY {0}
	ORDER BY {0} ASC;
	""".format(pivot, tile_ids, countries, dates)
	
	data = query_database(query, show_query=True)
	
	#insert the CTR and a javascript formatted date
	impressions = []
	for day in data:
		day = list(day)
		ctr = round((day[2] / float(day[1])) * 100, 5) if day[1] != 0 else 0
		
		if pivot == 'date':
			js_date = "Date.UTC({0}, {1}, {2})".format(day[0].year, day[0].month, day[0].day)
		else:
			js_date = False
		
		if pivot == 'country_code':
			if day[0] in cache['countries']['code_to_country']:
				day[0] = cache['countries']['code_to_country'][day[0]]
			else:
				day[0] = "Error/Possible Spam"
		
		eng = engagement(day[4], day[2])
		egrade = engagement_grade(eng)
		impressions.append([day[0], day[1], day[2], str(ctr)+"%", day[3], day[4], eng, egrade, js_date])
	
	if pivot == 'country_code':
		impressions = sorted(impressions, key=lambda x: x[0])
	
	return impressions

######### Data transformations ###########

def parse_selectors(args):
	"""Parses a set of url parameters into a useful dictionary of sets for scanning the cache"""
	
	selectors = {}
	
	#remove un-useful selectors
	not_useful = set(['pivot', 'country_name']) #things that aren't columns
	
	#format selectors to sets for fast lookup
	for arg, value in args.iteritems():
		if arg == 'id':
			selectors[arg] = [int(x) for x in value.split(',')]
		else:
			selectors[arg] = value.split(',')
		
		selectors[arg] = set(selectors[arg])
	
	return selectors

def convert_impressions_data_for_graph(data):
	"""Converts the output of get_daily_impressions_data to a format useful in Highcharts"""
	#data arrives as a list of lists, each sublist having 8 elements
	
	#way too much code repetition here
	column_names = ['Date', "Impressions", "Clicks", "CTR", "Pins", "Blocks", "Engagement", "Grade"]
	
	list_of_dates = ["Date.UTC({0}, {1}, {2})".format(x[0].year, x[0].month-1, x[0].day) for x in data]
	
	js_data = [[x, []] for x in column_names if x not in ['Date', 'Grade']]
	
	for row_index, row in enumerate(data):
		for n, cell in enumerate(row[1:-2]): #ignore date
			if (type(cell) == str) and cell.endswith("%"):
				cell = cell[:-1]
			js_data[n][1].append([list_of_dates[row_index], cell])
	
	return js_data

######### Other SQLish meta-data #########

def get_row_count(table_name):
	"""Gets the row count for a specific table.
	Will be slow for big tables"""
	print "Counting... (may take a while)"
	
	query = "SELECT COUNT(*) FROM " + table_name + ";"
	return query_database(query)
