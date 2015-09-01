#!/usr/bin/python
# -*- coding: utf8 -*-

#Useful module for grabbing data from Amazon Redshift and manipulating it
# - Most functions either have a docstring or are fairly self explanatory
# - There's another module needed called login but that contains a password
#	so isn't listed here.
# - All functions require a cursor and/or cache to be set up beforehand

from collections import defaultdict
from os import path
from json import load, dump, dumps
from datetime import datetime, timedelta
from codecs import open as copen
from pdb import set_trace
from re import search
from itertools import chain

import psycopg2
from login import login_string #login details, not committed
from isoweek import Week
from ago import human, delta2dict

verbose = False #debug setting

############# Basic caching and setup ##################

def cursor():
	"""Creates a cursor on the db for querying"""
	conn = psycopg2.connect(login_string)
	return conn.cursor()

def tile_cache_cursor():
	"""Creates a new cursor which can access the new db with updated tile information"""
	new_login_string = login_string.replace('tiles-prod-redshift.prod.mozaws.net', 'rds.tiles.prod.mozaws.net')
	conn = psycopg2.connect(new_login_string)
	return conn.cursor()

def build_tiles_cache(cursor, cache_cursor):
	"""Saves a version of the tiles as JSON to the local directory. Only does this once every 24 hours.
	Needs two cursors:
	 - One simple cursor to the main tiles db
	 - Another to get updated tile information
	"""
	
	#check if it actually needs updating
	redownload = False
	try:
		if not path.isfile("tiles.cache"):
			redownload = True #does it even exist
		else:
			with copen("tiles.cache", 'r', 'utf8') as f:
				cache = load(f)          #get the timestamp and convert to datetime
				last_updated = datetime.strptime(cache['last_updated'], "%Y-%m-%d %H:%M:%S.%f") 
				if (datetime.now()-last_updated).days > 0:
					redownload = True
	except Exception: #yolo
		redownload = True
	
	if redownload:
		print "Refreshing tiles cache from remote server (will take ~2 seconds)...",
		#get the tiles
		cache_cursor.execute("SELECT * FROM tiles;")
		tiles = cache_cursor.fetchall()
		#get the countries
		print "done"
		
		#A quick fix to this problem is just to assume that there's an entry (even if blank)
		#for each country for each tile. 
		
		countries = get_all_countries_from_server(cursor)
		
		#now insert into a dictionary object that will be nicely serializeable
		cache = {}
		for tile in tiles:
			cache[tile[0]] = {
				'target_url': tile[1],
				'bg_color': tile[2],
				'title': tile[3],
				'type': tile[4],
				'locale': tile[5],
				'adgroup_id': tile[6],
				'image_uri': tile[7],
				'enhanced_image_uri': tile[8],
				'created_at': unicode(tile[9]),
				'countries': countries
			}
		cache['last_updated'] = unicode(datetime.now())
		with copen('tiles.cache', 'w', 'utf8') as f:
			dump(cache, f)
		print "done"
	else:
		print "Finished loading tiles cache"
	
	del cache['last_updated'] #not useful when querying
	return cache

def build_mozilla_tile_list(cache):
	"""Finds mozilla tiles in the cache. This is complicated because there is currently no separate Advertiser/Client table.
	Returns an object with them"""
	
	mozilla_tiles = [
		{
			"name": "Webmaker Mobile App",
			"url_must_match": [
				"http://www.webmaker.org?ref=Webmaker_Launch&utm_campaign=Webmaker_Launch?utm_source=directory-tiles&utm_medium=tiles&utm_term=V1utm_campaign=Webmaker_Launch"
			]
		},
		{
			"name": "Make Firefox Your Default Browser",
			"url_must_match": [
				"https://support.mozilla.org/kb/make-firefox-your-default-browser?utm_source=directory-tiles&utm_medium=tiles&utm_content=DefaultV1&utm_campaign=Win10"
			]
		},
		{
			"name": "Privacy Tips",
			"url_must_match": [
				"https://www.mozilla.org/privacy/tips/?utm_source=directory-tiles&utm_medium=tiles&utm_content=PrivacyV1&utm_campaign=desktop"
			]
		},
		{
			"name": "Follow us on Twitter",
			"url_must_match": [
				"http://mzl.la/1U7wxPL", "http://mzl.la/1ILjvWb", "http://mzl.la/1KAsk2A"
			]
		},
		{
			"name": "Pocket for Firefox",
			"url_must_match": [
				"https://www.mozilla.org/firefox/pocket/?utm_source=directory-tiles&utm_medium=tiles&utm_term=v1&utm_campaign=desktop"
			]
		},
		{
			"name": "Newsletter_Directory",
			"url_must_match": [
				'https://www.mozilla.org/newsletter?utm_source=directory-tiles&utm_medium=tiles&utm_term=v1&utm_campaign=desktop'
			]
		},
		{
			"name": "Follow us on social",
			"url_must_match": ['http://mzl.la/1Vg8Hmk', 'http://mzl.la/1Mdhxxu', 'http://mzl.la/1RGAtsX', 'http://mzl.la/1KdVzbh', 'http://mzl.la/1HC2Bps']
		},
		{
			"name": "Foxyeah",
			"url_must_match": [
				"https://foxyeah.mozilla.org/?utm_source=directory-tiles&utm_medium=tiles&utm_campaign=sc-2015-foxyeah&utm_content=send-invite"
			]
		},
		{
			"name": "Foxyeah #2",
			"url_must_match": ["http://mzl.la/1HqQv3A"]
		},
		{
			"name": "Customize Firefox",
			"url_must_match": ["fastestfirefox.com", "https://addons.mozilla.org/en-US/android/"]
		},
		{
			"name": "Firefox 10th Anniversary",
			"url_must_match": ["https://www.mozilla.com/firefox/independent/?utm_source=directory-tiles&utm_medium=directory-tiles&utm_campaign=FX10"]
		},
		{
			'name': "Firefox for Android DT v1",
			'url_must_match': ['https://play.google.com/store/apps/details?id=org.mozilla.firefox&referrer=utm_source%3Dmozilla%26utm_medium%3Dbanner%26utm_campaign%3Ddesktop01']
		},
		{
			'name': "Firefox for Android DT v2",
			'url_must_match': ["http://android.www.mozilla.com/firefox/android/?utm_source=directory-tiles&utm_medium=tiles&utm_campaign=sc-2015-fennec&utm_content=phone-in-hand"]
		},
		{
			'name': "Firefox for Android ST",
			'url_must_match': [
				'https://www.mozilla.org/firefox/android/?utm_source=suggested-tiles&utm_medium=tiles&utm_content=androidenthusiasts&utm_campaign=firefoxforandroid',
				'https://www.mozilla.org/firefox/android/?utm_source=suggested-tiles&utm_medium=tiles&utm_content=mobileproviders&utm_campaign=firefoxforandroid',
				'https://www.mozilla.org/firefox/android/?utm_source=suggested-tiles&utm_medium=tiles&utm_content=mozillafans&utm_campaign=firefoxforandroid'
			]
		},
		{
			"name": 'Firefox Help and Support',
			"title_must_match": ['Firefox Help and Support'],
		},
		{
			"name": "Firefox Marketplace",
			"url_must_match": ['marketplace.firefox.com']
		},
		{
			"name": "Firefox Sync",
			"title_must_match": ["Firefox Sync"]
		},
		{
			"name": "Firefox Hello",
			"title_must_match": [
				'Firefox Hello', 
			]
		},
		{
			"name": "Get Smart on Privacy",
			'url_must_match': ['https://www.mozilla.com/privacy/tips?utm_source=firefox&utm_medium=directorytile&utm_campaign=DPD15']
		},
		{
			"name": "Lightbeam",
			"title_must_match": ['Lightbeam']
		},
		{
			"name": "Mozilla",
			"url_must_match": ["https://www.mozilla.com/en-US/?utm_source=directory-tiles&utm_medium=firefox-browser", "https://www.mozilla.org/en-US/?utm_source=directory-tiles&utm_medium=firefox-browser"]
		},
		{
			"name": "Mozilla Advocacy",
			"title_must_match": ["Mozilla Advocacy"]
		},
		{
			"name": "Mozilla Community",
			'url_must_match': ['http://contribute.mozilla.org/', 'http://mozilla.de/gemeinschaft/index.html']
		},
		{
			"name": "MDN Suggested",
			"url_must_match": [
				"https://developer.mozilla.org/Learn?utm_campaign=default&utm_source=mozilla&utm_medium=firefox-suggested-tile&utm_content=MozCat_WebLearner",
				"https://developer.mozilla.org/?utm_campaign=default&utm_source=mozilla&utm_medium=firefox-suggested-tile&utm_content=MozCat_Mozilla_Sites",
				"https://developer.mozilla.org/?utm_campaign=default&utm_source=mozilla&utm_medium=firefox-suggested-tile&utm_content=MozCat_WebDev"
			]
		},
		{
			"name": "MDN Directory",
			"url_must_match": [
				"https://developer.mozilla.org/en-GB/?utm_source=mozilla&utm_medium=firefox-tile&utm_campaign=default",
				"https://developer.mozilla.org/en-US/?utm_source=mozilla&utm_medium=firefox-tile&utm_campaign=default",
				"https://developer.mozilla.org/es/?utm_source=mozilla&utm_medium=firefox-tile&utm_campaign=default",
				"https://developer.mozilla.org/pt-BR/?utm_source=mozilla&utm_medium=firefox-tile&utm_campaign=default",
				"*https://developer.mozilla.org/en-US/",
				"https://developer.mozilla.org/ru/?utm_source=mozilla&utm_medium=firefox-tile&utm_campaign=default",
				"https://developer.mozilla.org/de/?utm_source=mozilla&utm_medium=firefox-tile&utm_campaign=default",
				"https://developer.mozilla.org/ja/?utm_source=mozilla&utm_medium=firefox-tile&utm_campaign=default",
				"https://developer.mozilla.org/pl/?utm_source=mozilla&utm_medium=firefox-tile&utm_campaign=default",
				"https://developer.mozilla.org/fr/?utm_source=mozilla&utm_medium=firefox-tile&utm_campaign=default",
				"*https://developer.mozilla.org/",
				"*https://developer.mozilla.org",
			]
		},
		{
			"name": "Mozilla Festival",
			"url_must_match": ["http://2014.mozillafestival.org/"]
		},
		{
			"name": "Mozilla Manifesto",
			"url_must_match": ["mozilla.org/about/manifesto", "https://www.mozilla.org/en-US/about/manifesto/"]
		},
		{
			"name": "Privacy Principles",
			'url_must_match': ["http://europe.mozilla.org/privacy/you"]
		},
		{
			"name": "Protect Net Neutrality",
			"title_must_match": ["Protect Net Neutrality"]
		},
		{
			"name": "Support Mozilla",
			"title_must_match": ["Support Mozilla"]
		},
		{
			"name": "The Mozilla Project",
			"title_must_match": ['The Mozilla Project']
		},
		{
			"name": "The Open Standard",
			'url_must_match': ['https://openstandard.mozilla.org/']
		},
		{
			"name": "Webmaker",
			'url_must_match': ["https://webmaker.org/"]
		},
		{
			"name": '"A brand new tiles experience"',
			'url_must_match': ['https://www.mozilla.com/firefox/tiles']
		},
		{
			"name": "Stop Surveillance",
			"title_must_match": ['Stop Surveillance']
		},
		{
			"name": "Get smart on mass surveillance",
			"title_must_match": ['Get smart on mass surveillance']
		},
		{
			"name": "Fennec Tiles",
			"id_must_match": ["629", "630", "631", "632"] #https://bugzilla.mozilla.org/show_bug.cgi?id=1131774
		}
	]
	
	#make sure ruleset is OK
	for x in mozilla_tiles:
		for k, v in x.iteritems():
			if 'must' in k:
				if type(v) != list: #everything apart from the name must have a list as the value
					print "Error in ruleset!"
					return False
	
	#get sponsored tiles first so we know which ones to ignore
	sponsored = set(get_sponsored_client_list(cache))
	
	#find matches
	now = datetime.now()
	ago = datetime(1900,1,1) #base date
	
	for x in range(len(mozilla_tiles)):
		mozilla_tiles[x]['last_modified'] = ago
		if len(mozilla_tiles[x]) > 1: #must have some sort of rule definition
			mozilla_tiles[x]['ids'] = [] #add container
			test_count = len([y for y in mozilla_tiles[x] if 'match' in y])

			for tile_id, tile_info in cache.iteritems():
				tests_passed = 0
				if 'url_must_match' in mozilla_tiles[x]:
					for matcher in mozilla_tiles[x]['url_must_match']:
						if matcher.startswith('*'): #must be exact
							if matcher[1:] == tile_info['target_url']:
								tests_passed += 1
								break
						else:
							if matcher in tile_info['target_url']:
								tests_passed += 1
								break
				
				if 'title_must_match' in mozilla_tiles[x]:
					for matcher in mozilla_tiles[x]['title_must_match']:
						if matcher in tile_info['title']:
							tests_passed += 1
							break
				
				if 'id_must_match' in mozilla_tiles[x]:
					for matcher in mozilla_tiles[x]['id_must_match']:
						if str(tile_id) == matcher:
							tests_passed += 1 #hackish but works for:
							break #https://bugzilla.mozilla.org/show_bug.cgi?id=1131774
				
				if tests_passed >= test_count:
					mozilla_tiles[x]['ids'].append(tile_id)
					
					#also put in last modified
					created_at = datetime.strptime(tile_info['created_at'], "%Y-%m-%d %H:%M:%S.%f")
					if created_at > mozilla_tiles[x]['last_modified']:
						mozilla_tiles[x]['last_modified'] = created_at
						mozilla_tiles[x]['ago'] = human(created_at, precision=3, past_tense='{0} ago')
			
			mozilla_tiles[x]['ids'] = set(mozilla_tiles[x]['ids'])
	
	#output what is left uncategorized
	already = set(chain.from_iterable([x['ids'] for x in mozilla_tiles if 'ids' in x]))
	
	for tile_id, tile_info in cache.iteritems():
		if tile_id not in already:
			if tile_info['title'] not in sponsored:
				if tile_info['type'] != 'organic':
					print "Error! Not categorized!", tile_id, tile_info['title'], tile_info['target_url']

	#check that nothing was caught by more than one
	if verbose:
		for tile in mozilla_tiles:
			for other_tile in mozilla_tiles:
				if tile['name'] != other_tile['name']:
					same = set(tile['ids']).intersection(other_tile['ids'])
					if len(same) > 0:
						print "Warning! {0} are in {1} and {2}".format(same, tile['name'], other_tile['name'])
	
	return mozilla_tiles

def get_all_countries_from_server(cursor):
	"""Gets a list of all countries in the database"""
	
	print "Refreshing a list of all countries from remote server (will take ~1 second)...",
	query = "SELECT country_name FROM countries"
	cursor.execute(query)
	print "done"
	countries = [x[0] for x in cursor.fetchall()]
	
	return countries

######### Querying client/tile data ###########

def get_all_locales(cache):
	"""Gets a list of all locales and prints them out to the terminal"""
	locales = sorted(list(set([x['locale'] for x in cache.itervalues()])))
	return locales

def get_locales_per_client(cache, client):
	"""Gets a list of locales that apply to a particular tile"""
	locales = set()
	for tile in cache.itervalues():
		if client in tile['title']:
			locales.update([tile['locale']])
	return locales

def get_sponsored_client_list(cache):
	"""Gets a list of clients and adds mozilla to the end"""
	#get all tiles
	clients = set()
	for x in cache.itervalues():
		if (x['type'] in "sponsored") or (x['title'] == "Yahoo"):
			if "/" not in x['title']:
				clients.update([x['title']])
	
	clients.update(['Mozilla'])
	return sorted(list(clients))

def get_tile_meta_data(cache, tile_id):
	"""Gets the entry for a specific tile/tiles in the tiles database and returns it as a list of lists"""
	
	if ("," in tile_id) or (type(tile_id) == list):
		if type(tile_id) != list:
			tile_id = tile_id.split(",")
		
		#get the fields for the first tile
		metadata_table = defaultdict(set)
		for x in tile_id:
			for field, value in cache[x].iteritems():
				if type(value) != list:
					value = [value]
				metadata_table[field].update(value)
			metadata_table['ids'].update([x])
		
		metadata_table = [[x[0], ", ".join([unicode(z) for z in list(x[1])])] if x[0] != 'countries' else [x[0], len(x[1])] for x in metadata_table.items()]		
	else:
		metadata_table = sorted([list(x) for x in cache[tile_id].items()])
		metadata_table[1][1] = len(metadata_table[1][1]) #collapse countries
		metadata_table.insert(0, ["id", tile_id]) #insert id
	return metadata_table

def get_mozilla_meta_data(cache, mozilla_tiles, campaign_name=False, locale=False):
	"""Compiles all the tile entries for a campaign in the moz tiles object and returns them as a list of lists to be entabulated"""
	data = {
		'Tile IDs': [],
		'Locales': set(),
		'Campaign start date': datetime.now(),
		'Campaign': campaign_name if campaign_name else "All Campaigns"
	}
	
	for campaign in mozilla_tiles:
		if campaign_name:
			if campaign['name'] != campaign_name:
				continue
		for tile_id in campaign['ids']:
			tile = cache[tile_id]
			if locale:
				if locale != tile['locale']:
					continue
			data['Tile IDs'].append(tile_id)
			data["Locales"].update([tile['locale']])
			created_at = datetime.strptime(tile['created_at'], "%Y-%m-%d %H:%M:%S.%f")
			if data['Campaign start date'] > created_at:
				data['Campaign start date'] = created_at
	
	#now turn into a list of lists
	data["Locales"] = ", ".join(sorted(list(data['Locales'])))
	data['Tile IDs'] = ", ".join(sorted(data['Tile IDs']))
	data = sorted(data.items())
	return data

def get_client_meta_data(cache, client=False, locale=False):
	"""Compiles all the client tiles entries in the tiles database and returns them as a list of lists to be entabulated"""
	
	data = {
		'Tile IDs': [],
		'Locales': set(),
		'Client start date': datetime.now(),
		'client': client
	}
	client = client.lower()
	
	for tile_id, tile in cache.iteritems():
		if client in tile['title'].lower():
			if locale:
				if locale != tile['locale']:
					continue
			data['Tile IDs'].append(tile_id)
			data["Locales"].update([tile['locale']])
			created_at = datetime.strptime(tile['created_at'], "%Y-%m-%d %H:%M:%S.%f")
			if data['Client start date'] > created_at:
				data['Client start date'] = created_at
	
	#now turn into a list of lists
	data["Locales"] = ", ".join(sorted(list(data['Locales'])))
	data['Tile IDs'] = ", ".join(sorted(data['Tile IDs']))
	data = sorted(data.items())
	return data

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

def get_tiles_from_client_in_locale(cache, client, locale):
	"""Gets a list of tiles that run in a particular locale for a particular client"""
	tiles = []
	for tile_id, tile in cache.iteritems():
		if client in tile['title']:
			if locale == tile['locale']:
				tile['id'] = tile_id
				tiles.append(tile)
	return tiles

def get_tiles_per_client(cache, client):
	"""Gets a list of tiles for a particular client"""
	tiles = []
	for tile_id, tile in cache.iteritems():
		if client in tile['title']:
			tile['id'] = tile_id
			tiles.append(tile)
	return tiles

def get_tile_ids_per_client(cache, client, locale=False):
	"""Gets a list of tile ids per client"""
	ids = []
	for tile_id, tile in cache.iteritems():
		if client in tile['title']:
			if locale:
				if locale != tile['locale']:
					continue
			ids.append(tile_id)
	return ids

def get_countries_per_client(cache, mozilla_tiles=False, client=False, locale=False, campaign=False):
	"""Gets a list of countries that a particular tile ID ran in"""
	
	countries = set()
	if client == "Mozilla":
		#get relevant tile ids, accomodating for the campaign
		tiles = []
		for tile in mozilla_tiles:
			if campaign:
				if campaign != tile['name']:
					continue
			tiles += tile['ids']
		#get the countries that they run in 
		for tile_id in tiles:
			info = cache[tile_id]
			if locale:
				if locale != info['locale']:
					continue
			countries.update(info['countries'])
	else:
		for x in cache.itervalues():
			if client in x['title']:
				if locale:
					if locale == x['locale']:
						countries.update(x['countries'])
				else:
					countries.update(x['countries'])
	
	return sorted(countries)

def get_countries_per_tile(cache, tile_id):
	"""Given a tile id it returns the country list"""
	if "," in tile_id:
		tile_id = tile_id.split(",")
		countries = set()
		for x in tile_id:
			countries.update(cache[x]['countries'])
		return list(countries)
	else:
		return cache[tile_id]['countries']

def get_all_countries(cache):
	"""Just gets all possible countries"""
	countries = set()
	for tile, attribs in cache.iteritems():
		countries.update(attribs['countries'])
	countries = sorted(list(countries))
	return countries

def get_client_attributes(cursor, cache, client):
	"""For a given tile id, gets all possible locales, countries and campaign start dates.
	This is useful for the drop down menus.
	Accepts an integer tile id and returns a dictionary of lists"""
	
	attributes = {
		'locales': [],
		'countries': [],
	}
	
	attributes['countries'] = get_countries_per_client(cache, client=client)
	attributes['locales'] = get_locales_per_client(cache, client)
	
	return attributes

########## Engagement ###########

def create_engagement_graph_data(impressions_data, time_unit):
	"""Converts the output of get_temporal_engagement to a format useful in Highcharts"""
	
	#recieves: [[day, ..., eng], ...]
	#outputs: [[js_day, eng], ...]
	
	#data is weirdly unsorted, so we have to convert everything to a
	#date object, then to the js string format
	
	graph_data = []
	for entry in impressions_data:
		date = entry[0].split("-")
		if time_unit == 'monthly':
			date = datetime(int(date[0]), int(date[1]), 1)
		elif time_unit == 'weekly':
			#have to work out start date (?) of week from the week number
			d = Week(int(date[0]), int(date[1])).monday()
			date = datetime(int(date[0]), d.month, d.day)
		else: #daily
			date = datetime(int(date[0]), int(date[1]), int(date[2]))
		
		engagement = entry[-1]
		graph_data.append([date, engagement])

	graph_data = sorted(graph_data)
	graph_data = [["Date.UTC({0}, {1}, {2})".format(x[0].year, x[0].month-1, x[0].day), x[1]] for x in graph_data]
	
	return graph_data

def get_temporal_engagement(cursor, time_delimiter):
	"""Daily overall engagement"""
	
	query = """
	SELECT date, SUM(impressions) as impressions, SUM(blocked) as blocks, SUM(clicks) as clicks
	FROM impression_stats_daily
	GROUP BY date
	ORDER BY date ASC
	"""
	print query
	cursor.execute(query)
	
	data = defaultdict(lambda: [0,0,0,0]) #impressions, blocks, clicks, engagement
	
	if time_delimiter == "monthly":
		col_name = "Month"
		for row in cursor.fetchall():
			month = str(row[0].month)
			if len(month) == 1:
				month = "0"+str(month)
			formatted_date = "{0}-{1}".format(row[0].year, month)
			data[formatted_date][0] += row[1]
			data[formatted_date][1] += row[2]
			data[formatted_date][2] += row[3]
	elif time_delimiter == "weekly":
		col_name = "Week"
		for row in cursor.fetchall():
			week = str(row[0].isocalendar()[1])
			if len(week) == 1:
				week = "0"+week
			formatted_date = "{0}-{1}".format(row[0].year, week)
			if formatted_date != "2014-01": #strange bug
				data[formatted_date][0] += row[1]
				data[formatted_date][1] += row[2]
				data[formatted_date][2] += row[3]
	else:
		#daily
		col_name = "Day"
		for row in cursor.fetchall():
			formatted_date = row[0].strftime("%Y-%m-%d")
			data[formatted_date][0] += row[1]
			data[formatted_date][1] += row[2]
			data[formatted_date][2] += row[3]
	
	data = sorted(data.items()) #sort by date ascending
	formatted = [[col_name, "Impressions", "Blocks", "Clicks", "Engagement"]]
	
	#now add in engagement and format nicely with [date, x, y, z...]
	for date in data:
		formatted.append([
				date[0],
				date[1][0],
				date[1][1],
				date[1][2],
				engagement(date[1][1], date[1][2])
			])
	
	return formatted

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

def add_engagement_metrics(impressions_data):
	"""Accepts some daily impressions data.
	Tacks on a few more columns with various engagement metrics for testing"""
	
	#Impressions data arrives in the format:
	#[[date, impressions, clicks, ctr%, pins, blocks, js_date]]
	
	#remove the js_date
	impressions_data = [x[:-1] for x in impressions_data]
	
	#method 1: pin rank
	impressions_data = sorted(impressions_data, key=lambda x: x[4], reverse=True)
	impressions_data = [x+[n] for n, x in enumerate(impressions_data)]
	
	#method 2: ranked CTR
	impressions_data = sorted(impressions_data, key=lambda x: x[3], reverse=True)
	impressions_data = [x+[n] for n, x in enumerate(impressions_data)]
	
	#method 3: pins per block
	impressions_data = [x+[round(x[4]/float(x[5]), 2)] if x[5] != 0 else x+[0] for x in impressions_data]
	
	#method 4: combined interaction
	impressions_data = sorted(impressions_data, key=lambda x: x[5], reverse=True)
	impressions_data = [x+[round(sum([x[6], x[7], n])/3.0, 2)] for n, x in enumerate(impressions_data)]
	
	#method 7: baselined block/click ratio
	impressions_data=  [x+[engagement(x[5], x[2])] for x in impressions_data]
	
	return impressions_data

########## Querying impressions data ###########

def get_daily_impressions_data_for_engagement(cursor, client=False):
	"""Gets aggregated impressions grouped by day for the engagement page, pulled from a cache"""
	
	#check cache
	redownload = False
	try:
		if not path.isfile("engagement.cache"):
			redownload = True #does it even exist
		else:
			with copen("engagement.cache", 'r', 'utf8') as f:
				cache = load(f)          #get the timestamp and convert to datetime
				last_updated = datetime.strptime(cache['last_updated'], "%Y-%m-%d %H:%M:%S.%f") 
				if (datetime.now()-last_updated).days > 0:
					redownload = True
	except Exception: #yolo
		redownload = True
	
	if not redownload:
		if not client:
			client = "Dashlane"
		return cache[client]
	else:
		query = u"""
			CREATE TEMPORARY TABLE clients (
			  pattern VARCHAR(20)
			);
			
			INSERT INTO clients VALUES
			('%BBC%'), 
			('%Booking.com%'), 
			('%CITIZENFOUR%'), 
			('%CVS Health%'), 
			('%Dashlane%'), 
			('%Outbrain Sphere%'), 
			('%PagesJaunes%'), 
			('%Trulia%'),
			('%TurboTax%'),
			('%ImpôtRapide%'),
			('%WIRED%');
			
			SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks, title
			FROM impression_stats_daily
			INNER JOIN tiles ON tiles.id = impression_stats_daily.tile_id
			JOIN clients c on (tiles.title LIKE c.pattern)
			GROUP BY date, title
			ORDER BY date ASC
		""".format(client.lower())
		
		print "Creating engagement cache..."
		cursor.execute(query)
		data = cursor.fetchall()
		
		impressions = {}
		
		for day in data:
			day = list(day)
			client = day[-1] #store client
			day = day[:-1] #remove client
			ctr = round((day[2] / float(day[1])) * 100, 5) if day[1] != 0 else 0
			js_date = "Date.UTC({0}, {1}, {2})".format(day[0].year, day[0].month, day[0].day)
			day = ["{0}-{1}-{2}".format(day[0].year, day[0].month, day[0].day), day[1], day[2], str(ctr)+"%", day[3], day[4], js_date]
			if client not in impressions:
				impressions[client] = []
			impressions[client].append(day)
		
		#sort
		for client in impressions:
			impressions[client] = sorted(impressions[client])
		
		impressions['last_updated'] = unicode(datetime.now())
		with copen('engagement.cache', 'w', 'utf8') as f:
			dump(impressions, f)
		print "done"
		
		if not client:
			client = "Dashlane"
		return impressions[client]

def get_daily_impressions_data(cursor, cache, tile_id=False, client=False, country='all', locale=False, tile_ids=False):
	"""Gets aggregated impressions grouped by day"""
	
	if tile_ids:
		#Compile together results from several tile ids, usually in the same campaign
		if type(tile_ids) in [str, unicode]:
			tile_ids = tile_ids.split(",")
		if len(tile_ids) == 1:
			where = "= {0}".format(list(tile_ids)[0])
		else:
			where = ", ".join(tile_ids)
			where = "in ({0})".format(where)
		query = u"""
			SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
			FROM impression_stats_daily
			WHERE tile_id {0}
			GROUP BY date
			ORDER BY date ASC
		""".format(where)
	elif client: #get all tiles
		if country == "all": #all countries
			if locale:
				tile_ids = get_tile_ids_per_client(cache, client, locale=locale)
				query = u"""
				SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				WHERE tile_id in ({0})
				GROUP BY date
				ORDER BY date ASC
				""".format(", ".join(tile_ids))
			else:
				tile_ids = get_tile_ids_per_client(cache, client)
				query = u"""
				SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				WHERE tile_id in ({0})
				GROUP BY date
				ORDER BY date ASC
				""".format(", ".join(tile_ids))
		else: #specific country
			if locale:
				tile_ids = get_tile_ids_per_client(cache, client, locale=locale)
				query = u"""
				SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
				WHERE tile_id in ({0}) AND country_name = '{1}' AND impression_stats_daily.locale = '{2}'
				GROUP BY date
				ORDER BY date ASC
				""".format(", ".join(tile_ids), country, locale)
			else:
				tile_ids = get_tile_ids_per_client(cache, client)
				query = u"""
				SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
				WHERE tile_id in ({0}) AND country_name = '{1}'
				GROUP BY date
				ORDER BY date ASC
				""".format(", ".join(tile_ids), country)
	else: #specific tile
		if country == "all": #all countries
			query = u"""
			SELECT date, SUM(impressions) as impressions, SUM(clicks) as clicks, SUM(pinned) as pinned, SUM(blocked) as blocked
			FROM impression_stats_daily
			WHERE tile_id = {0}
			GROUP BY date
			ORDER BY date ASC;
			""".format(tile_id)
		else: #specific country
			query = u"""
			SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pinned, SUM (blocked) AS blocked
			FROM impression_stats_daily
			INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
			WHERE tile_id = {0}
			AND countries.country_name = '{1}'
			GROUP BY impression_stats_daily.date, countries.country_name
			ORDER BY impression_stats_daily.date ASC;
			""".format(tile_id, country)
	
	print query
	cursor.execute(query)
	data = cursor.fetchall()
	
	#insert the CTR and a javascript formatted date
	impressions = []
	for day in data:
		day = list(day)
		ctr = round((day[2] / float(day[1])) * 100, 5) if day[1] != 0 else 0
		js_date = "Date.UTC({0}, {1}, {2})".format(day[0].year, day[0].month, day[0].day)
		eng = engagement(day[4], day[2])
		egrade = engagement_grade(eng)
		impressions.append([day[0], day[1], day[2], str(ctr)+"%", day[3], day[4], eng, egrade, js_date])
	return impressions

def get_countries_impressions_data(cursor, cache, tile_id=False, start_date=False, end_date=False, client=False, locale=False, tile_ids=False):
	"""Gets aggregated impressions grouped by each country. This is for the country-by-country analysis"""
	
	#construct WHERE clause using parameters
	where = []
	if start_date:
		where.append(u"DATE >= '{0}'".format(start_date))
	if end_date:
		where.append(u"DATE <= '{0}'".format(end_date))
	if tile_id:
		where.append(u"tile_id = " + tile_id)
	if client:
		if client != 'Mozilla':
			client_tile_ids = get_tile_ids_per_client(cache, client)
			where.append(u"tile_id in ({0})".format(", ".join(client_tile_ids)))
	if locale:
		where.append(u"locale = '{0}'".format(locale))
	if tile_ids:
		if len(tile_ids) == 1:
			where.append(u"tile_id = {0}".format(list(tile_ids)[0]))
		else:
			tile_ids = ", ".join(tile_ids)
			where.append(u"tile_id in ({0})".format(tile_ids))
	
	#put into single line
	if len(where) == 1:
		where = "WHERE " + where[0]
	else:
		where = "WHERE " + ' AND '.join(where)
	
	query = u"""
			SELECT countries.country_name, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
			FROM impression_stats_daily
			INNER JOIN countries on countries.country_code = impression_stats_daily.country_code
			{0}
			GROUP BY countries.country_name
			ORDER BY impressions DESC;
	""".format(where)
	print query
	
	cursor.execute(query)
	data = cursor.fetchall()
	
	#insert the CTR
	impressions = []
	for day in data:
		day = list(day)
		ctr = round((day[2] / float(day[1])) * 100, 5) if day[1] != 0 else 0
		eng = engagement(day[4], day[2])
		egrade = engagement_grade(eng)
		impressions.append([day[0], day[1], day[2], str(ctr)+"%", day[3], day[4], eng, egrade]) #why doesn't insert() work
	return impressions

def get_locale_impressions_data(cursor, cache, client=False, start_date=False, end_date=False, country=False, tile_id=False, tile_ids=False):
	"""Get impressions data locale-by-locale"""
	
	print "Got:"
	print "Tile ID: {0}".format(tile_id)
	print "Tile IDs: {0}".format(tile_ids)
	
	#construct WHERE clause using parameters
	where = []
	if start_date:
		where.append(u"DATE >= '{0}'".format(start_date))
	if end_date:
		where.append(u"DATE <= '{0}'".format(end_date))
	if tile_id:
		where.append(u"tile_id = " + tile_id)
	if client:
		if client != 'Mozilla':
			client_tile_ids = get_tile_ids_per_client(cache, client)
			where.append(u"tile_id in ({0})".format(", ".join(client_tile_ids)))
	if country:
		where.append(u"country_name = '{0}'".format(country))
	if tile_ids:
		if type(tile_ids) != list:
			tile_ids = [str(int(x.strip())) for x in tile_ids.split(',')]
		
		print "Tile IDs now: {0}".format(tile_ids)
		
		if len(tile_ids) == 1:
			where.append(u"tile_id = {0}".format(list(tile_ids)[0]))
		else:
			tile_ids = ", ".join(tile_ids)
			where.append(u"tile_id in ({0})".format(tile_ids))
	
	#put into single line
	if len(where) == 1:
		where = "WHERE " + where[0]
	else:
		where = "WHERE " + ' AND '.join(where)
	
	query = u"""
			SELECT impression_stats_daily.locale, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
			FROM impression_stats_daily
			INNER JOIN countries on countries.country_code = impression_stats_daily.country_code
			{0}
			GROUP BY impression_stats_daily.locale
			ORDER BY impressions DESC;
	""".format(where)
	print query
	
	cursor.execute(query)
	data = cursor.fetchall()
	
	#insert the CTR and a javascript formatted date
	impressions = []
	for day in data:
		day = list(day)
		ctr = round((day[2] / float(day[1])) * 100, 5) if day[1] != 0 else 0
		eng = engagement(day[4], day[2])
		egrade = engagement_grade(eng)
		impressions.append([day[0], day[1], day[2], str(ctr)+"%", day[3], day[4], eng, egrade])
	
	return impressions

def get_country_impressions_data(cursor, country=False):
	"""Gets country impressions data for the 'countries' page. Not to be confused with the country-by-country analysis"""
	
	if country:
		query = u"""SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
				WHERE country_name = '{0}'
				GROUP BY date
				ORDER BY date""".format(country)
	else:
		query = u"""SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				GROUP BY date
				ORDER BY date"""
	
	print query
	cursor.execute(query)
	data = cursor.fetchall()
	
	#insert the CTR and Engagement
	impressions = []
	for day in data:
		day = list(day)
		ctr = round((day[2] / float(day[1])) * 100, 5) if day[1] != 0 else 0
		eng = engagement(day[4], day[2])
		impressions.append([day[0], day[1], day[2], str(ctr), day[3], day[4], eng])
	
	#convert to a JSON ish format for highcharts
	#or at least something that can be easily understood by jinja
	#	        series: [{
	#            name: 'Impressions',
	#            data: [
	#                [Date.UTC(1970,  9, 27), 0   ],
	#                [Date.UTC(1971,  5, 12), 0   ]
	#            ]}]
	
	#I think the easiest way to do this is to create a structure like:
	#[
	#	[
	#		'impressions',
	#		[
	#			["Date.UTC(2014, 1, 1)", 123],
	#			["Date.UTC(2014, 1, 2)", 345]
	#		]
	#	],
	#	...
	#]
	# and then simply iterate through that in jinja
	
	#column_names = [x[0] for x in cursor.description]
	column_names = ["impressions", "clicks", 'CTR', 'pins', 'blocks', 'engagement']
	list_of_dates = ["Date.UTC({0}, {1}, {2})".format(x[0].year, x[0].month-1, x[0].day) for x in data]
	js_data = [[x, []] for x in column_names if x != 'date']
	
	for row_index, row in enumerate(impressions):
		for n, cell in enumerate(row[1:]): #ignore date
			js_data[n][1].append([list_of_dates[row_index], cell])
	
	return js_data

def get_overview_data(cursor, mozilla_tiles, cache, country=False, locale=False, start_date=False, end_date=False):
	"""Grabs overview data"""
	
	#sort out the parameters
	if country or locale or start_date or end_date:
		where = []
		if country:
			where.append(u"country_name = '" + country + "'")
		if locale:
			where.append(u"locale = '" + locale + "'")
		if start_date:
			where.append(u"date >= '" + start_date + "'")
		if end_date:
			where.append(u"date <= '" + end_date + "'")
		query = u"""
			SELECT tile_id, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
			FROM impression_stats_daily
			INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
			{0}
			GROUP BY tile_id
		""".format("WHERE " + " AND ".join(where))
	else:
		query = u"""
			SELECT tile_id, sum(impressions) as impressions, sum(clicks) as clicks, sum(pinned) as pins, sum(blocked) as blocks
			FROM impression_stats_daily
			GROUP BY tile_id
		"""
	
	#grab the data from the server
	print query
	cursor.execute(query)
	data = cursor.fetchall()
	
	#enter each tile's data into a dictionary referenced by id
	#this just makes things easier later when 
	tile_data = {}
	for entry in data:
		tile_data[unicode(entry[0])] = entry[1:]
	
	#aggregate and add the data to the relevant tile entry
	for t in range(len(mozilla_tiles)):
		if 'client' not in mozilla_tiles[t]: #i.e. can't be sponsored tiles
			totals = [0 for x in range(len(data[0])-1)] #create blank holder for totals per client
			earliest_created_at = datetime.now() 
			
			for tile in mozilla_tiles[t]['ids']:
				if tile in tile_data: #some don't yet have data
					stats = tile_data[tile]
					for n, x in enumerate(stats):
						if (type(x) != unicode) and (type(x) != str):
							totals[n] += x #aggregate
				
				#get the earliest created_at for each campaign
				created_at = datetime.strptime(cache[tile]['created_at'], "%Y-%m-%d %H:%M:%S.%f")
				if created_at < earliest_created_at:
					earliest_created_at = created_at
			
			#insert the CTR
			ctr = round((totals[1] / float(totals[0])) * 100, 5) if totals[0] != 0 else 0
			totals.insert(2, ctr)
			
			#append the engagement
			totals.append(engagement(totals[4], totals[1]))
			
			#append the engagement grade
			totals.append(engagement_grade(totals[-1]))
			
			#add
			mozilla_tiles[t]['stats'] = totals
			mozilla_tiles[t]['created_at'] = "{0}-{1}-{2}".format(earliest_created_at.year, earliest_created_at.month, earliest_created_at.day)

	#now we have to add in all the paid tiles
	clients = set([x for x in get_sponsored_client_list(cache) if x != 'Mozilla'])
	
	invalid = set()
	
	for client in clients:
		to_add = {
			'name': client,
			'stats': [0 for x in range(len(data[0])-1)],
			'earliest_created_at': datetime.now()
		}
		
		for tile, stats in tile_data.iteritems():
			if tile in cache:
				title = cache[tile]['title']
				if title == client:
					for n, x in enumerate(stats):
						if (type(x) != unicode) and (type(x) != str):
							to_add['stats'][n] += x #aggregate
					created_at = datetime.strptime(cache[tile]['created_at'], "%Y-%m-%d %H:%M:%S.%f")
					if created_at < to_add['earliest_created_at']:
						to_add['earliest_created_at'] = created_at
			else:
				#make a list of non-existant tiles
				invalid.update([tile])
		
		ctr = round((to_add['stats'][1] / float(to_add['stats'][0])) * 100, 5) if to_add['stats'][0] != 0 else 0
		to_add['stats'].insert(2, ctr)
		
		#append the engagement
		to_add['stats'].append(engagement(to_add['stats'][4], to_add['stats'][1]))
		
		#append engagement grade
		to_add['stats'].append(engagement_grade(to_add['stats'][-1]))
		
		to_add['client'] = True
		to_add['created_at'] = "{0}-{1}-{2}".format(to_add['earliest_created_at'].year, to_add['earliest_created_at'].month, to_add['earliest_created_at'].day)
		
		#now remove the existing one and replace it
		mozilla_tiles = [x for x in mozilla_tiles if x['name'] != to_add['name']]
		mozilla_tiles.append(to_add)
	
	#if len(invalid) > 0:
	#	#ping matthew with an alert for non-existant tiles
	#	error_report(invalid)
	print list(invalid)
	
	return mozilla_tiles

def get_yahoo_overview(cursor, yahoo_filter, country=False, locale=False, start_date=False, end_date=False):
	"""Creates an overview of yahoo data"""
	
	#First need to create a full list of yahoo tile ids
	#And create a useful id to category payload
	id_to_category = {}
	all_yahoo_tile_ids = []
	for x in yahoo_filter['categories']:
		ids = x[1].split(',')
		all_yahoo_tile_ids += ids
		for i in ids: 
			id_to_category[i] = x[0]
	
	#Decide the parameters (like country, date etc)
	where = []
	if country: where.append(u"country_name = '" + country + "'")
	if locale: where.append(u"locale = '" + locale + "'")
	if start_date: where.append(u"date >= '" + start_date + "'")
	if end_date: where.append(u"date <= '" + end_date + "'")
	if where:
		where = 'AND ' + ' AND '.join(where)
	else:
		where = ""
	
	#Next setup a query
	query = """
		SELECT
			tile_id,
			SUM (impressions) AS impressions,
			SUM (clicks) AS clicks,
			SUM (pinned) AS pins,
			SUM (blocked) AS blocks
		FROM
			impression_stats_daily
		INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
		WHERE
			tile_id in ({0})
			{1}
		GROUP BY tile_id
	""".format(", ".join(all_yahoo_tile_ids), where)
	
	#execute it
	print query
	cursor.execute(query)
	
	#Now aggregate based on category
	category_aggregate = {}
	for row in cursor.fetchall():
		category = id_to_category[unicode(row[0])]
		data = row[1:]
		if category not in category_aggregate:
			category_aggregate[category] = data
		else:
			category_aggregate[category] = [sum(x) for x in zip(data, category_aggregate[category])]
	
	#now add in some extra metrics and format correctly
	table_data = []
	for cat, row_data in category_aggregate.iteritems():
		
		#insert the CTR
		ctr = round((row_data[1] / float(row_data[0])) * 100, 5) if row_data[0] != 0 else 0
		row_data.insert(2, ctr)
		
		#append the engagement
		row_data.append(engagement(row_data[4], row_data[1]))
		
		#append the engagement grade
		row_data.append(engagement_grade(row_data[-1]))
		
		table_data.append({
				'name': cat,
				'stats': row_data
			})
	
	return table_data

######### Data transformations ###########

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

def get_column_headers(cursor, table_name):
	"""Gets the column headings for a table"""
	cursor.execute("select * from " + table_name + " LIMIT 1;")
	colnames = [desc[0] for desc in cursor.description]
	return colnames

def get_row_count(cursor, table_name):
	"""Gets the row count for a specific table.
	Will be slow for big tables"""
	print "Counting... (may take a while)"
	cursor.execute("SELECT COUNT(*) FROM " + table_name + ";")
	return cursor.fetchall()
	
######### Auxiliary functionality #########

def error_report(what):
	"""Emails Matthew with whatever went wrong. Not currently working."""
	from smtplib import SMTP #rare one-time import
	SERVER = "localhost"
	FROM = "error_report@zenko"
	TO = ["mruttley@mozilla.com"] # must be a list
	SUBJECT = "Error Report"
	TEXT = dumps(what)
	message = """\
	From: %s
	To: %s
	Subject: %s
	
	%s
	""" % (FROM, ", ".join(TO), SUBJECT, TEXT)
	# Send the mail
	server = SMTP(SERVER)
	server.sendmail(FROM, TO, message)
	server.quit()
