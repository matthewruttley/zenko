#!/usr/bin/python
# -*- coding: utf8 -*-

#Useful module for grabbing data from Amazon Redshift and manipulating it
# - Most functions either have a docstring or are fairly self explanatory
# - There's another module needed called login but that contains a password
#	so isn't listed here.
# - All functions require a cursor to be set up beforehand
# - Typical way to use this:
# > import redshift
# > cursor = redshift.cursor()
# > redshift.search_tiles(cursor, "youtube", 0, True)
# ...(prints out lots of stuff)

from collections import defaultdict
import psycopg2
from login import login_string #login details, not committed

def cursor():
	"""Creates a cursor on the db for querying"""
	conn = psycopg2.connect(login_string)
	return conn.cursor()

def get_locales_per_client(cursor, client):
	"""Gets a list of locales that apply to a particular tile"""
	client = client.lower().encode()
	query = "SELECT DISTINCT locale FROM tiles WHERE lower(title) LIKE '%{0}%';".format(client)
	cursor.execute(query)
	locales = cursor.fetchall()
	locales = [x[0] for x in locales]
	return locales

def get_client_list(cursor):
	"""Gets a list of clients"""
	#get all tiles
	tiles = get_all_tiles(cursor)
	clients = {}
	for tile in tiles:
		if "/" not in tile[3]:
			clients[tile[0]] = tile[3].decode('utf8') #there will be lots of duplicate client names
	clients = sorted(set(clients.values()))
	return clients

def get_sponsored_client_list(cursor):
	"""Gets a list of sponsored clients"""
	#get all tiles
	tiles = get_all_tiles(cursor)
	clients = {}
	for tile in tiles:
		if tile[4] == "sponsored":
			if "/" not in tile[3]:
				clients[tile[0]] = tile[3].decode('utf8') #there will be lots of duplicate client names
	clients = sorted(set(clients.values()))
	return clients

def get_impressions(cursor, timeframe, tile_id):
	"""Gets and aggregates impressions in a certain time frame"""
	query = """
		SELECT
			date,
			SUM(impressions) as impressions,
			SUM(clicks) as clicks,
			SUM(pinned) as pinned,
			SUM(blocked) as blocked
		FROM 
			impression_stats_daily
		WHERE
			tile_id = 647
		GROUP BY date
		ORDER BY date ASC;
	"""
	cursor.execute(query)
	impressions = cursor.fetchall()
	return impressions

def get_tile_meta_data(cursor, tile_id):
	query = """
				SELECT * FROM tiles WHERE id = {0};
	""".format(tile_id)
	cursor.execute(query)
	metadata = cursor.fetchall()
	colnames = [desc[0] for desc in cursor.description]
	#now make a nice table with attr --> vaulue
	metadata_table = zip(colnames, metadata[0])
	return metadata_table

def get_tiles_from_client_in_locale(cursor, client, locale):
	"""Gets a list of tiles that run in a particular locale for a particular client"""
	query = """
				SELECT
					id, target_url, created_at
				FROM
					tiles
				WHERE
					lower(title) like '%{0}%'
				and locale = '{1}'
				ORDER BY
					created_at DESC;
	""".format(client.lower(), locale)
	cursor.execute(query)
	tiles = cursor.fetchall()
	return tiles

def get_tables(cursor):
	"""Gets a list of useful tables, ignores anything that looks too system-y"""
	ignore = "|".join([
			"pg_",
			"sql_",
			"stl_",
			"stv_",
			"temp_",
			"svl_",
			"svv_",
			"table_",
			"systable_",
			"role_",
			"column_",
			"applicable_",
			"view_",
			"loadview",
			"enabled",
			"padb",
			"usage",
			"check_c",
			"constraint",
			"triggered",
			"distributions",
			"key_column",
			"referential_",
			"information_schema_catalog_name"
		])
	cursor.execute("select relname from pg_class where relname !~ '^(" + ignore + ")';")
	table_list = cursor.fetchall()
	table_list = [x[0] for x in table_list] #cleanup
	#sort and return
	return sorted(table_list)

def get_all_tiles(cursor):
	"""Gets a list of all the tiles currently on the server"""
	cursor.execute("select * from tiles;")
	tiles = cursor.fetchall()
	return tiles

def get_countries_per_client(cursor, client_name):
	"""Gets a list of countries that a particular tile ID ran in"""
	
	query = """
							SELECT DISTINCT
								countries.country_name
							FROM
								tiles
							INNER JOIN
								impression_stats_daily ON tiles.id = impression_stats_daily.tile_id
							INNER JOIN 
								countries on countries.country_code = impression_stats_daily.country_code
							WHERE
								lower(tiles.title) LIKE '%dashlane%'
								AND impression_stats_daily.blocked + impression_stats_daily.clicks + impression_stats_daily.impressions + impression_stats_daily.pinned > 0
							ORDER BY countries.country_name ASC;
	""".format(client_name)
	
	cursor.execute(query)
	countries = cursor.fetchall()
	countries = [x[0] for x in countries] #query returns a list of lists so have to break out of that
	
	return countries

def get_locales_per_tile_id(cursor, tile_id):
	"""Gets a list of locales that a particular tile ID ran in"""
	query = """
				SELECT DISTINCT
					locale
				FROM
					tiles
				WHERE
					id = {0};
	""".format(tile_id)
	
	cursor.execute(query)
	locales = cursor.fetchall()
	
	return locales

def get_start_dates_per_tile_id(cursor, tile_id):
	"""Gets a list of campaign start dates that a particular tile ID ran in"""
	query = """
				SELECT
					created_at
				FROM
					tiles
				WHERE
					id = {0};
	""".format(tile_id)
	
	cursor.execute(query)
	start_dates = cursor.fetchall()
	
	return locales

def get_tile_attributes(cursor, client):
	"""For a given tile id, gets all possible locales, countries and campaign start dates.
	This is useful for the drop down menus.
	Accepts an integer tile id and returns a dictionary of lists"""
	
	attributes = {
		'locales': [],
		'countries': [],
	}
	
	attributes['countries'] = get_countries_per_client(cursor, client)
	attributes['locales'] = get_locales_per_client(cursor, client)
	
	return attributes

def get_tile_stats(cursor, title, locale):
	"""Gets stats for a few days"""
	#<td>{{day.date}}</td>
	#<td>{{day.impressions}}</td>
	#<td>{{day.clicks}}</td>
	#<td>{{day.ctr}}</td>
	#<td>{{day.pins}}</td>
	#<td>{{day.blocks}}</td>
	days = []
	query = """SELECT
					date,
					SUM(impressions) AS impressions,
					SUM(clicks) as clicks,
					SUM(pinned) as pins,
					SUM(blocked) as blocks
				FROM
					impression_stats_daily
				WHERE
					tile_id = 647
				GROUP BY date
				ORDER BY date ASC;"""
	cursor.execute(query)
	for entry in cursor.fetchall():
		day_dict = {
			"date": entry[0],
			"impressions": entry[1],
			"clicks": entry[2],
			"ctr": "?",
			"pins": entry[3],
			"blocks": entry[4],
			
		}
		days.append(day_dict)
	return days

def search_tiles(cursor, search_term, locale=0, display=False):
	"""Searches tiles and prints out relevant ones.
	The search term will search both the client and target url
	The locale is by default 0 (everything) or can be set optionally
	(use get_all_locales for a list of them)
	If display is set to True then it will print the results to
	the terminal, otherwise it will return a list. """
	tiles = get_all_tiles(cursor)
	to_return = []
	if display:
		headers = get_column_headers(cursor, "tiles")
		print headers
	for x in tiles:
		if (search_term in x[3].lower().decode('utf8')) or (search_term in x[1].lower().decode('utf8')):
			if locale != 0:
				if x[-2] == locale:
					if display:
						print x
					else:
						to_return.append(x)
			else:	
				if display:
					print x
				else:
					to_return.append(x)
	if not display: return to_return

def get_column_headers(cursor, table_name):
	"""Gets the column headings for a table"""
	cursor.execute("select * from " + table_name + " LIMIT 1;")
	colnames = [desc[0] for desc in cursor.description]
	return colnames

def get_all_locales(cursor):
	"""Gets a list of all locales and prints them out to the terminal"""
	tiles = get_all_tiles(cursor)
	locales = defaultdict(int)
	for x in tiles:
		locales[x[-2]] += 1
	locales = sorted(locales.items(), key=lambda x: x[1], reverse=True)
	for x in locales:
		print x

def get_row_count(cursor, table_name):
	"""Gets the row count for a specific table.
	Will be slow for big tables"""
	print "Counting... (may take a while)"
	cursor.execute("SELECT COUNT(*) FROM " + table_name)
	return cursor.fetchall()
	
