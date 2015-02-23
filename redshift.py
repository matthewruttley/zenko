#!/usr/bin/python
# -*- coding: utf8 -*-

#Useful module for grabbing data from Amazon Redshift and manipulating it
# - Most functions either have a docstring or are fairly self explanatory
# - There's another module needed called login but that contains a password
#	so isn't listed here.
# - All functions require a cursor and/or cache to be set up beforehand

from collections import defaultdict
from os import path
from json import load, dump
from datetime import datetime
from codecs import open as copen
from pdb import set_trace
from re import search
from itertools import chain

import psycopg2
from login import login_string #login details, not committed

############# Basic caching and setup ##################

def cursor():
	"""Creates a cursor on the db for querying"""
	conn = psycopg2.connect(login_string)
	return conn.cursor()

def build_tiles_cache(cursor):
	"""Saves a version of the tiles as JSON to the local directory. Only does this once every 24 hours"""
	
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
		print "Refreshing tiles cache from remote server (will take ~2 seconds)..."
		#get the tiles
		cursor.execute("SELECT * FROM tiles;")
		tiles = cursor.fetchall()
		#get the countries
		print "done"
		print "Refreshing tile country cache from remote server (could take up to ~29 seconds)..."
		cursor.execute("""
			SELECT DISTINCT
				tiles.id,
				countries.country_name
			FROM
				tiles
			INNER JOIN impression_stats_daily ON tiles.id = impression_stats_daily.tile_id
			INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
			ORDER BY
				countries.country_name ASC;
			""")
		tile_countries = defaultdict(set)
		for tile, country in cursor.fetchall():
			tile_countries[tile].update([country])
		#now insert into a dictionary object that will be nicely serializeable
		cache = {}
		for tile in tiles:
			cache[tile[0]] = {
				'target_url': tile[1],
				'bg_color': tile[2],
				'title': tile[3],
				'type': tile[4],
				'image_uri': tile[5],
				'enhanced_image_uri': tile[6],
				'locale': tile[7],
				'created_at': unicode(tile[8]),
				'countries': sorted(list(tile_countries[tile[0]])) if tile[0] in tile_countries else []
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
			"name": "Customize Firefox",
			"url_must_match": ["fastestfirefox.com", "https://addons.mozilla.org/en-US/android/"]
		},
		{
			"name": "Firefox 10th Anniversary",
			"url_must_match": ["https://www.mozilla.com/firefox/independent/?utm_source=directory-tiles&utm_medium=directory-tiles&utm_campaign=FX10"]
		},
		{
			'name': "Firefox for Android",
			'url_must_match': ['https://play.google.com/store/apps/details?id=org.mozilla.firefox&referrer=utm_source%3Dmozilla%26utm_medium%3Dbanner%26utm_campaign%3Ddesktop01']
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
			'url_must_match': ['http://contribute.mozilla.org/']
		},
		{
			"name": "Mozilla Developer Network",
			"url_must_match": ["developer.mozilla.org"]
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
		}
	]
	
	#make sure ruleset is OK
	for x in mozilla_tiles:
		for k, v in x.iteritems():
			if 'must' in k:
				if type(v) != list:
					print "Error in ruleset!"
					return False
	
	#get sponsored tiles first so we know which ones to ignore
	sponsored = set(get_sponsored_client_list(cache))
	
	#find matches
	for x in range(len(mozilla_tiles)):
		if len(mozilla_tiles[x]) > 1: #must have some sort of rule definition
			mozilla_tiles[x]['ids'] = [] #add container
			
			#print "Processing Rule {0} ({1})".format(x, mozilla_tiles[x]['name'])
			
			#number of tests that need passing
			test_count = len([y for y in mozilla_tiles[x] if 'match' in y])
			
			#print "Tests to pass: {0}".format(test_count)
			
			for tile_id, tile_info in cache.iteritems():
				tests_passed = 0
				if 'url_must_match' in mozilla_tiles[x]:
					for matcher in mozilla_tiles[x]['url_must_match']:
						if matcher in tile_info['target_url']:
							tests_passed += 1
							break
				
				if 'title_must_match' in mozilla_tiles[x]:
					for matcher in mozilla_tiles[x]['title_must_match']:
						if matcher in tile_info['title']:
							tests_passed += 1
							break
				
				if tests_passed >= test_count:
					mozilla_tiles[x]['ids'].append(tile_id)
					#print tile_id, tile_info['title'], tile_info['target_url']
			
			mozilla_tiles[x]['ids'] = set(mozilla_tiles[x]['ids'])
			#print "{0} tiles matched".format(len(mozilla_tiles[x]['ids']))
	
	#output what is left uncategorized
	already = set(chain.from_iterable([x['ids'] for x in mozilla_tiles if 'ids' in x]))
	
	#print "Already categorized {0}/{1} tiles".format(len(already), len(cache))
	
	for tile_id, tile_info in cache.iteritems():
		if tile_id not in already:
			if tile_info['title'] not in sponsored:
				if tile_info['type'] != 'organic':
					print tile_id, tile_info['title'], tile_info['target_url']

	#check that nothing was caught by more than one
	for tile in mozilla_tiles:
		for other_tile in mozilla_tiles:
			if tile['name'] != other_tile['name']:
				same = set(tile['ids']).intersection(other_tile['ids'])
				if len(same) > 0:
					print "Warning! {0} are in {1} and {2}".format(same, tile['name'], other_tile['name'])
	
	return mozilla_tiles

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
		if x['type'] == "sponsored":
			if "/" not in x['title']:
				clients.update([x['title']])
	
	clients.update(['Mozilla'])
	return sorted(list(clients))

def get_tile_meta_data(cache, tile_id):
	"""Gets the entry for a specific tile in the tiles database and returns it as a list of lists"""
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
	
	#method 5: click fallout
	impressions_data = [x+[""] for x in impressions_data]
	
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

def get_daily_impressions_data(cursor, tile_id=False, client=False, country='all', locale=False, tile_ids=False):
	"""Gets aggregated impressions grouped by day"""
	
	if tile_ids:
		#Compile together results from several tile ids, usually in the same campaign
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
				query = u"""
				SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				INNER JOIN tiles ON tiles.id = impression_stats_daily.tile_id
				WHERE LOWER (title) LIKE '%{0}%' AND tiles.locale = '{1}'
				GROUP BY date
				ORDER BY date ASC
				""".format(client.lower(), locale)
			else:
				query = u"""
				SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				INNER JOIN tiles ON tiles.id = impression_stats_daily.tile_id
				WHERE LOWER (title) LIKE '%{0}%'
				GROUP BY date
				ORDER BY date ASC
				""".format(client.lower())
		else: #specific country
			if locale:
				query = u"""
				SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
				INNER JOIN tiles ON tiles.id = impression_stats_daily.tile_id
				WHERE LOWER (title) LIKE '%{0}%' AND country_name = '{1}' AND tiles.locale = '{2}'
				GROUP BY date
				ORDER BY date ASC
				""".format(client.lower(), country, locale)
			else:
				query = u"""
				SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
				INNER JOIN tiles ON tiles.id = impression_stats_daily.tile_id
				WHERE LOWER (title) LIKE '%{0}%' AND country_name = '{1}'
				GROUP BY date
				ORDER BY date ASC
				""".format(client.lower(), country)
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
		impressions.append([day[0], day[1], day[2], str(ctr)+"%", day[3], day[4], js_date])
	return impressions

def get_countries_impressions_data(cursor, tile_id=False, start_date=False, end_date=False, client=False, locale=False, tile_ids=False):
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
			where.append(u"LOWER (title) LIKE '%{0}%'".format(client.lower()))
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
			INNER JOIN tiles on tiles.id = impression_stats_daily.tile_id
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
		impressions.append([day[0], day[1], day[2], str(ctr)+"%", day[3], day[4]]) #why doesn't insert() work
	return impressions

def get_locale_impressions_data(cursor, client=False, start_date=False, end_date=False, country=False, tile_id=False, tile_ids=False):
	"""Get impressions data locale-by-locale"""
	
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
			where.append(u"LOWER (title) LIKE '%{0}%'".format(client.lower()))
	if country:
		where.append(u"country_name = '{0}'".format(country))
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
			SELECT impression_stats_daily.locale, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
			FROM impression_stats_daily
			INNER JOIN tiles on tiles.id = impression_stats_daily.tile_id
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
		impressions.append([day[0], day[1], day[2], str(ctr)+"%", day[3], day[4]])
	
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
	
	#insert the CTR
	impressions = []
	for day in data:
		day = list(day)
		ctr = round((day[2] / float(day[1])) * 100, 5) if day[1] != 0 else 0
		impressions.append([day[0], day[1], day[2], str(ctr), day[3], day[4]])
	
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
	
	column_names = [x[0] for x in cursor.description]
	list_of_dates = ["Date.UTC({0}, {1}, {2})".format(x[0].year, x[0].month-1, x[0].day) for x in data]
	js_data = [[x, []] for x in column_names if x != 'date']
	
	for row_index, row in enumerate(data):
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
			
			#add
			mozilla_tiles[t]['stats'] = totals
			mozilla_tiles[t]['created_at'] = "{0}-{1}-{2}".format(earliest_created_at.year, earliest_created_at.month, earliest_created_at.day)

	#now we have to add in all the paid tiles
	clients = set([x for x in get_sponsored_client_list(cache) if x != 'Mozilla'])
	
	invalid = set(["-1", "999", "16903", "16910", "20000", "900000"])
	for client in clients:
		to_add = {
			'name': client,
			'stats': [0 for x in range(len(data[0])-1)],
			'earliest_created_at': datetime.now()
		}
		
		for tile, stats in tile_data.iteritems():
			if tile not in invalid: #some legacy thing apparently
				title = cache[tile]['title']
				if title == client:
					for n, x in enumerate(stats):
						if (type(x) != unicode) and (type(x) != str):
							to_add['stats'][n] += x #aggregate
					created_at = datetime.strptime(cache[tile]['created_at'], "%Y-%m-%d %H:%M:%S.%f")
					if created_at < to_add['earliest_created_at']:
						to_add['earliest_created_at'] = created_at
		
		ctr = round((to_add['stats'][1] / float(to_add['stats'][0])) * 100, 5) if to_add['stats'][0] != 0 else 0
		to_add['stats'].insert(2, ctr)
		to_add['client'] = True
		to_add['created_at'] = "{0}-{1}-{2}".format(to_add['earliest_created_at'].year, to_add['earliest_created_at'].month, to_add['earliest_created_at'].day)
		
		#now remove the existing one and replace it
		mozilla_tiles = [x for x in mozilla_tiles if x['name'] != to_add['name']]
		mozilla_tiles.append(to_add)
	
	return mozilla_tiles

######### Data transformations ###########

def convert_impressions_data_for_graph(data):
	"""Converts the output of get_daily_impressions_data to a format useful in Highcharts"""
	#data arrives as a list of lists, each sublist having 6 elements
	
	#way too much code repetition here
	column_names = ['Date', "Impressions", "Clicks", "CTR", "Pins", "Blocks"]
	list_of_dates = ["Date.UTC({0}, {1}, {2})".format(x[0].year, x[0].month-1, x[0].day) for x in data]
	js_data = [[x, []] for x in column_names if x != 'Date']
	
	for row_index, row in enumerate(data):
		for n, cell in enumerate(row[1:-1]): #ignore date
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
	
