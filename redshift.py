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

def get_tile_locales(cursor, title):
	"""Gets a list of locales that apply to a particular tile"""
	tiles = search_tiles(cursor, title.lower())
	locales = [x[-2] for x in tiles]
	locales = sorted(list(set(locales)))
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
	cursor.execute("select * from " + table_name + ";")
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
	
