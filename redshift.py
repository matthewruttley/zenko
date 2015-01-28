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
		print "Refreshing tile country cache from remote server (will take ~15 seconds)..."
		cursor.execute("""
							SELECT DISTINCT
								tiles.id,
								countries.country_name
							FROM
								tiles
							INNER JOIN
								impression_stats_daily ON tiles.id = impression_stats_daily.tile_id
							INNER JOIN 
								countries on countries.country_code = impression_stats_daily.country_code
							WHERE
								tiles.type = 'sponsored'
								AND tiles.title not like '%/%'
								AND impression_stats_daily.blocked + impression_stats_daily.clicks + impression_stats_daily.impressions + impression_stats_daily.pinned > 0
							ORDER BY countries.country_name ASC;""")
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
		print "Loaded tiles cache..."
	
	del cache['last_updated'] #not useful when querying
	return cache

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
	"""Gets a list of clients"""
	#get all tiles
	clients = set()
	for x in cache.itervalues():
		if x['type'] == "sponsored":
			if "/" not in x['title']:
				clients.update([x['title']])
	return sorted(list(clients))

def get_tile_meta_data(cache, tile_id):
	"""Gets the entry for a specific tile in the tiles database and returns it as a list of lists"""
	metadata_table = sorted([list(x) for x in cache[tile_id].items()])
	metadata_table[1][1] = len(metadata_table[1][1]) #collapse countries
	metadata_table.insert(0, ["id", tile_id]) #insert id
	return metadata_table

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

def get_countries_per_client(cache, client=False, locale=False):
	"""Gets a list of countries that a particular tile ID ran in"""
	countries = set()
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

def get_client_attributes(cursor, cache, client):
	"""For a given tile id, gets all possible locales, countries and campaign start dates.
	This is useful for the drop down menus.
	Accepts an integer tile id and returns a dictionary of lists"""
	
	attributes = {
		'locales': [],
		'countries': [],
	}
	
	attributes['countries'] = get_countries_per_client(cache, client)
	attributes['locales'] = get_locales_per_client(cache, client)
	
	return attributes

########## Querying impressions data ###########

def get_daily_impressions_data(cursor, tile_id=False, client=False, country='all', locale=False):
	"""Gets aggregated impressions grouped by day"""
	
	if client: #get all tiles
		if country == "all": #all countries
			if locale:
				query = """
				SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				INNER JOIN tiles ON tiles.id = impression_stats_daily.tile_id
				WHERE LOWER (title) LIKE '%{0}%' AND tiles.locale = '{1}'
				GROUP BY date
				ORDER BY date ASC
				""".format(client.lower(), locale)
			else:
				query = """
				SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				INNER JOIN tiles ON tiles.id = impression_stats_daily.tile_id
				WHERE LOWER (title) LIKE '%{0}%'
				GROUP BY date
				ORDER BY date ASC
				""".format(client.lower())
		else: #specific country
			if locale:
				query = """
				SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
				INNER JOIN tiles ON tiles.id = impression_stats_daily.tile_id
				WHERE LOWER (title) LIKE '%{0}%' AND country_name = '{1}' AND tiles.locale = '{2}'
				GROUP BY date
				ORDER BY date ASC
				""".format(client.lower(), country, locale)
			else:
				query = """
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
			query = """
			SELECT date, SUM(impressions) as impressions, SUM(clicks) as clicks, SUM(pinned) as pinned, SUM(blocked) as blocked
			FROM impression_stats_daily
			WHERE tile_id = {0}
			GROUP BY date
			ORDER BY date ASC;
			""".format(tile_id)
		else: #specific country
			query = """
			SELECT date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pinned, SUM (blocked) AS blocked
			FROM impression_stats_daily
			INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
			WHERE tile_id = {0}
			AND countries.country_name = '{1}'
			GROUP BY impression_stats_daily.date, countries.country_name
			ORDER BY impression_stats_daily.date ASC;
			""".format(tile_id, country)
	
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

def get_countries_impressions_data(cursor, tile_id=False, start_date=False, end_date=False, client=False, locale=False):
	"""Gets aggregated impressions grouped by each country"""
	
	if tile_id == False: #get all tiles
		if (start_date==False) and (end_date==False): #i.e. no date specified
			if locale:
				query = """
					SELECT countries.country_name, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
					FROM impression_stats_daily
					INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
					INNER JOIN tiles ON tiles.id = impression_stats_daily.tile_id
					WHERE LOWER (title) LIKE '%{0}%' AND tiles.locale = '{1}'
					GROUP BY country_name
					ORDER BY impressions DESC
				""".format(client.lower(), locale)
			else:
				query = """
					SELECT countries.country_name, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
					FROM impression_stats_daily
					INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
					INNER JOIN tiles ON tiles.id = impression_stats_daily.tile_id
					WHERE LOWER (title) LIKE '%{0}%'
					GROUP BY country_name
					ORDER BY impressions DESC
				""".format(client.lower())
		else: #date specified
			if locale:
				query = """
					SELECT countries.country_name, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
					FROM impression_stats_daily
					INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
					INNER JOIN tiles ON tiles.id = impression_stats_daily.tile_id
					WHERE LOWER (title) LIKE '%{0}%' AND DATE >= '{1}' AND DATE <= '{2}' AND tiles.locale = '{3}'
					GROUP BY country_name
					ORDER BY impressions DESC
				""".format(client.lower(), start_date, end_date, locale)
			else:
				query = """
					SELECT countries.country_name, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
					FROM impression_stats_daily
					INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
					INNER JOIN tiles ON tiles.id = impression_stats_daily.tile_id
					WHERE LOWER (title) LIKE '%{0}%' AND DATE >= '{1}' AND DATE <= '{2}'
					GROUP BY country_name
					ORDER BY impressions DESC
				""".format(client.lower(), start_date, end_date)
	else:
		#specific tile
		if (start_date==False) and (end_date==False): #i.e. no date specified
			query = """
				SELECT countries.country_name, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
				WHERE tile_id = {0}
				GROUP BY country_name
				ORDER BY impressions DESC
			""".format(tile_id)
		else:
			query = """
				SELECT countries.country_name, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
				FROM impression_stats_daily
				INNER JOIN countries ON countries.country_code = impression_stats_daily.country_code
				WHERE tile_id = {0} AND date >= '{1}' AND date <= '{2}'
				GROUP BY country_name
				ORDER BY impressions DESC
			""".format(tile_id, start_date, end_date)
	
	cursor.execute(query)
	data = cursor.fetchall()
	
	#insert the CTR
	impressions = []
	for day in data:
		day = list(day)
		ctr = round((day[2] / float(day[1])) * 100, 5) if day[1] != 0 else 0
		impressions.append([day[0], day[1], day[2], str(ctr)+"%", day[3], day[4]]) #why doesn't insert() work
	return impressions

def get_locale_impressions_data(cursor, client=False, start_date=False, end_date=False, country=False, tile_id=False):
	"""Get impressions data locale-by-locale"""

	#construct WHERE clause
	where = []
	if start_date:
		where.append("DATE >= '{0}'".format(start_date))
	if end_date:
		where.append("DATE <= '{0}'".format(end_date))
	if tile_id:
		where.append("impression_stats_daily.id = " + tile_id)
	if client:
		where.append("LOWER (title) LIKE '%{0}%'".format(client.lower()))
	if country:
		where.append("country_name = '{0}'".format(country))
	where = "WHERE " + ' AND '.join(where)
	
	query = """
			SELECT impression_stats_daily.locale, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
			FROM impression_stats_daily
			INNER JOIN tiles on tiles.id = impression_stats_daily.tile_id
			INNER JOIN countries on countries.country_code = impression_stats_daily.country_code
			{0}
			GROUP BY impression_stats_daily.locale
			ORDER BY impressions DESC;
	""".format(where)
	
	cursor.execute(query)
	data = cursor.fetchall()
	
	#insert the CTR and a javascript formatted date
	impressions = []
	for day in data:
		day = list(day)
		ctr = round((day[2] / float(day[1])) * 100, 5) if day[1] != 0 else 0
		impressions.append([day[0], day[1], day[2], str(ctr)+"%", day[3], day[4]])
	
	return impressions

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
	

















