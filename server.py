#Basic Python Server in Flask for showing reports
#Essentially put together from this page: http://flask.pocoo.org/docs/0.10/quickstart/
#Just run this like:
#> python server.py
#and then visit http://localhost:5000

from json import dump, dumps
from datetime import datetime
from pdb import set_trace
from webbrowser import open as open_webpage
from flask import Flask, render_template, request, make_response, Response
import redshift
from pdb import set_trace
app = Flask(__name__)

#set up database connection
cursor = redshift.cursor()
cache_cursor = redshift.tile_cache_cursor()
cache = redshift.build_tiles_cache(cursor, cache_cursor)
mozilla_tiles = redshift.build_mozilla_tile_list(cache)
#import redshift;cursor=redshift.cursor();cache_cursor = redshift.tile_cache_cursor();cache=redshift.build_tiles_cache(cursor, cache_cursor);mozilla_tiles = redshift.build_mozilla_tile_list(cache)

#Some custom filter stuff

@app.template_filter('thousands')
def add_commas(number):
	"""1234567890 ---> 1,234,567,890"""
	return "{:,}".format(number)

#Views

@app.route('/download_excel', methods=['GET', 'POST'])
def download_excel():
	"""Creates a file download from recieved post information"""
	data = request.form["data"]
	data = data.split("###")
	data = [[y.replace(",", "") for y in x.split("#")] for x in data]
	data = "\n".join([','.join(x) for x in data])
	#filename = request.form["filename"]
	response = make_response(data)
	response.headers["Content-Disposition"] = "attachment; filename=data.csv"
	return response	

@app.route('/daily_impressions')
def show_daily_impressions():
	"""Shows daily impressions for a tile_id/client"""
	
	tile_id = request.args.get("tile_id")
	tile_ids = request.args.get("tile_ids")
	country = request.args.get("country")
	client = request.args.get("client")
	locale = request.args.get("locale")
	campaign = request.args.get("campaign")
	
	#get a list of clients for the side bar
	clients = redshift.get_sponsored_client_list(cache)
	
	#get a list of possible locales and countries
	if client:
		countries = redshift.get_countries_per_client(cache, mozilla_tiles=mozilla_tiles, client=client, campaign=campaign, locale=locale)
	elif tile_ids:
		countries = redshift.get_countries_per_tile(cache, tile_ids)
	else:
		countries = redshift.get_countries_per_tile(cache, tile_id)
	
	#get some daily data
	if client:
		if client == 'Mozilla':
			if campaign: #special case since mozilla tiles are allowed to have particular campaigns
				tiles = [x['ids'] for x in mozilla_tiles if x['name'] == campaign][0]
				impressions_data = redshift.get_daily_impressions_data(cursor, cache, tile_ids=tiles)
				meta_data = redshift.get_mozilla_meta_data(cache, mozilla_tiles, campaign_name=campaign)
				client = 'Mozilla'
			else:
				#All Mozilla data
				tiles = [] #all tile IDs
				for x in mozilla_tiles:
					tiles += list(x['ids'])
				
				impressions_data = redshift.get_daily_impressions_data(cursor, cache, tile_ids=tiles)
				meta_data = redshift.get_mozilla_meta_data(cache, mozilla_tiles)
				client = 'Mozilla'
		else:
			if country:
				if locale:
					impressions_data = redshift.get_daily_impressions_data(cursor, cache, client=client, country=country, locale=locale)
				else:
					impressions_data = redshift.get_daily_impressions_data(cursor, cache, client=client, country=country)
			else:
				if locale:
					impressions_data = redshift.get_daily_impressions_data(cursor, cache, client=client, locale=locale)
				else:
					impressions_data = redshift.get_daily_impressions_data(cursor, cache, client=client)
			if locale:
				meta_data = redshift.get_client_meta_data(cache, client=client, locale=locale)
			else:
				meta_data = redshift.get_client_meta_data(cache, client=client)
		specific_tile = False
	else:
		if country:
			impressions_data = redshift.get_daily_impressions_data(cursor, cache, tile_id=tile_id, tile_ids=tile_ids, country=country)
		else:
			impressions_data = redshift.get_daily_impressions_data(cursor, cache, tile_id=tile_id, tile_ids=tile_ids)
		
		if tile_ids:
			meta_data = redshift.get_tile_meta_data(cache, tile_ids)
			client = [x[1] for x in meta_data if x[0] == 'title'][0]
			specific_tile = tile_ids
		else:
			meta_data = redshift.get_tile_meta_data(cache, tile_id)
			client = [x[1] for x in meta_data if x[0] == 'title'][0]
			specific_tile = tile_id
	
	#convert the data to be graph-able
	graph = redshift.convert_impressions_data_for_graph(impressions_data)
	
	#insert an error message if needed
	error = False
	if len(impressions_data) == 0:
		error = {
					"message": ["The Redshift Tiles server doesn't seem to be returning any data for this analysis.", "Sound odd? Please contact "],
					"contact": "mruttley@mozilla.com"
		}
	
	#render the template
	return render_template("index.html", clients=clients, client=client, meta_data=meta_data, countries=countries, impressions_data=impressions_data, country=country, tile_id=tile_id, locale=locale, specific_tile=specific_tile, impressions_data_graph=graph, campaign=campaign, error=error, tile_ids=tile_ids)

@app.route('/country_impressions')
def show_country_impressions():
	"""Shows impressions for a tile_id or client country-by-country"""
	
	#get a list of clients for the side bar
	clients = redshift.get_sponsored_client_list(cache)
	
	#get the GET variables
	tile_id = request.args.get("tile_id")
	tile_ids = request.args.get("tile_ids")
	start_date = request.args.get("start_date")
	end_date = request.args.get("end_date")
	client = request.args.get("client")
	locale = request.args.get("locale")
	campaign = request.args.get("campaign")
	
	#first get the relevant impressions data
	
	if client == 'Mozilla': #special case since mozilla tiles are allowed to have particular campaigns
		if campaign:
			print "Got campaign: {0}".format(campaign)
			tiles = [x['ids'] for x in mozilla_tiles if x['name'] == campaign][0]
			countries_impressions_data = redshift.get_countries_impressions_data(cursor, tile_ids=tiles, start_date=start_date, end_date=end_date, locale=locale)
			meta_data = redshift.get_mozilla_meta_data(cache, mozilla_tiles, campaign_name=campaign)
		else:
			#all mozilla tiles
			tiles = []
			for x in mozilla_tiles:
				tiles += x['ids']
			countries_impressions_data = redshift.get_countries_impressions_data(cursor, tile_ids=tiles, start_date=start_date, end_date=end_date, locale=locale)
			meta_data = redshift.get_mozilla_meta_data(cache, mozilla_tiles)
	else:
		if tile_ids:
			tile_ids = tile_ids.split(',')
		countries_impressions_data = redshift.get_countries_impressions_data(cursor, start_date=start_date, end_date=end_date, client=client, locale=locale, tile_id=tile_id, tile_ids=tile_ids)
	
	#get some meta data about the tile from the tiles database (including bounds for the slider)
	slider = {}
	if client:
		if locale:
			meta_data = redshift.get_client_meta_data(cache, client=client, locale=locale)
		else:
			meta_data = redshift.get_client_meta_data(cache, client=client)
		specific_tile = False
		start_bound = [x for x in meta_data if x[0] == 'Client start date'][0][1]
		end_bound = datetime.now()
	else: #specific tile
		
		if tile_id:
			meta_data = redshift.get_tile_meta_data(cache, tile_id)
			specific_tile = tile_id
		else:
			meta_data = redshift.get_tile_meta_data(cache, tile_ids)
			specific_tile = tile_ids
		
		client = [x[1] for x in meta_data if x[0] == 'title'][0]
		start_bound = [x for x in meta_data if x[0] == 'created_at'][0][1]
		end_bound = datetime.now()
	
	#now add in the start and end dates if specified, and format all dates according to javascript's style
	if start_date and end_date:
		#set the start and end dates of the tile
		start_date = [int(x) for x in start_date.split("-")]
		end_date = [int(x) for x in end_date.split("-")]
		start_date = datetime(start_date[0], start_date[1], start_date[2])
		end_date = datetime(end_date[0], end_date[1], end_date[2])
		slider = {
			'start_bound': "{0}, {1}, {2}".format(start_bound.year, start_bound.month-1, start_bound.day),
			'end_bound': "{0}, {1}, {2}".format(end_bound.year, end_bound.month-1, end_bound.day),
			'start_value': "{0}, {1}, {2}".format(start_date.year, start_date.month-1, start_date.day),
			'end_value': "{0}, {1}, {2}".format(end_date.year, end_date.month-1, end_date.day),
		}
	else:
		sb_type = type(start_bound)
		if (sb_type == str) or (sb_type == unicode):
			if "," in start_bound:
				start_bound = [x.strip() for x in start_bound.split(',')]
				start_bound = [datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f") for x in start_bound if x != ""]
				start_bound = min(start_bound)
			else:
				start_bound = datetime.strptime(start_bound, "%Y-%m-%d %H:%M:%S.%f")
		
		slider = {
			'start_bound': "{0}, {1}, {2}".format(start_bound.year, start_bound.month-1, start_bound.day),
			'end_bound': "{0}, {1}, {2}".format(end_bound.year, end_bound.month-1, end_bound.day),
			'start_value': "{0}, {1}, {2}".format(start_bound.year, start_bound.month-1, start_bound.day),
			'end_value': "{0}, {1}, {2}".format(end_bound.year, end_bound.month-1, end_bound.day),
		}
	
	#reset datatype
	if type(specific_tile) == list:
		specific_tile = ", ".join([str(x) for x in specific_tile])
	
	if type(tile_ids) == list:
		tile_ids = ",".join([str(x) for x in tile_ids])
	
	#render the template
	return render_template("index.html", clients=clients, client=client, meta_data=meta_data, countries_impressions_data=countries_impressions_data, slider=slider, specific_tile=specific_tile, locale=locale, campaign=campaign, tile_id=tile_id, tile_ids=tile_ids)

@app.route('/locale_impressions')
def show_locale_impressions():
	"""Shows impressions locale-by-locale"""
	
	client = request.args.get('client')
	start_date = request.args.get('start_date')
	end_date = request.args.get('end_date')
	country = request.args.get('country')
	tile_id = request.args.get('tile_id')
	tile_ids = request.args.get('tile_ids')
	campaign = request.args.get('campaign')

	#get a list of clients for the side bar
	clients = redshift.get_sponsored_client_list(cache)

	#get a list of possible locales and countries
	if client:
		countries = redshift.get_countries_per_client(cache, mozilla_tiles=mozilla_tiles, client=client, campaign=campaign)
	else:
		if tile_ids:
			countries = redshift.get_countries_per_tile(cache, tile_ids)
		else:
			countries = redshift.get_countries_per_tile(cache, tile_id)

	if client == 'Mozilla': #special case since mozilla tiles are allowed to have particular campaigns
		if campaign:
			tiles = [x['ids'] for x in mozilla_tiles if x['name'] == campaign][0]
			impressions_data = redshift.get_locale_impressions_data(cursor, tile_ids=tiles, start_date=start_date, end_date=end_date, country=country)
			meta_data = redshift.get_mozilla_meta_data(cache, mozilla_tiles, campaign_name=campaign)
		else:
			#all mozilla tiles
			tiles = []
			for x in mozilla_tiles:
				tiles += x['ids']
			impressions_data = redshift.get_locale_impressions_data(cursor, tile_ids=tiles, start_date=start_date, end_date=end_date, country=country)
			meta_data = redshift.get_mozilla_meta_data(cache, mozilla_tiles)
	else:
		impressions_data = redshift.get_locale_impressions_data(cursor=cursor, client=client, start_date=start_date, end_date=end_date, country=country, tile_id=tile_id, tile_ids=tile_ids)
	
	#get some meta data about the tile from the tiles database (including bounds for the slider)
	slider = {}
	if client:
		meta_data = redshift.get_client_meta_data(cache, client=client)
		specific_tile = False
		start_bound = [x for x in meta_data if x[0] == 'Client start date'][0][1]
		end_bound = datetime.now()
	else: #specific tile
		if tile_id:
			meta_data = redshift.get_tile_meta_data(cache, tile_id)
			specific_tile = tile_id
		else:
			meta_data = redshift.get_tile_meta_data(cache, tile_ids)
			specific_tile = tile_ids
		
		client = [x[1] for x in meta_data if x[0] == 'title'][0]
		start_bound = [x for x in meta_data if x[0] == 'created_at'][0][1]
		end_bound = datetime.now()
	
	#now add in the start and end dates if specified, and format all dates according to javascript's style
	if start_date and end_date:
		#set the start and end dates of the tile
		start_date = [int(x) for x in start_date.split("-")]
		end_date = [int(x) for x in end_date.split("-")]
		start_date = datetime(start_date[0], start_date[1], start_date[2])
		end_date = datetime(end_date[0], end_date[1], end_date[2])
		slider = {
			'start_bound': "{0}, {1}, {2}".format(start_bound.year, start_bound.month-1, start_bound.day),
			'end_bound': "{0}, {1}, {2}".format(end_bound.year, end_bound.month-1, end_bound.day),
			'start_value': "{0}, {1}, {2}".format(start_date.year, start_date.month-1, start_date.day),
			'end_value': "{0}, {1}, {2}".format(end_date.year, end_date.month-1, end_date.day),
		}
	else:
		sb_type = type(start_bound)
		if (sb_type == str) or (sb_type == unicode):
			if "," in start_bound:
				start_bound = [x.strip() for x in start_bound.split(',')]
				start_bound = [datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f") for x in start_bound if x != ""]
				start_bound = min(start_bound)
			else:
				start_bound = datetime.strptime(start_bound, "%Y-%m-%d %H:%M:%S.%f")
		
		slider = {
			'start_bound': "{0}, {1}, {2}".format(start_bound.year, start_bound.month-1, start_bound.day),
			'end_bound': "{0}, {1}, {2}".format(end_bound.year, end_bound.month-1, end_bound.day),
			'start_value': "{0}, {1}, {2}".format(start_bound.year, start_bound.month-1, start_bound.day),
			'end_value': "{0}, {1}, {2}".format(end_bound.year, end_bound.month-1, end_bound.day),
		}
	
	return render_template("index.html", client=client, clients=clients, start_date=start_date, countries=countries, end_date=end_date, country=country, tile_id=tile_id, specific_tile=specific_tile, locale_impressions_data=impressions_data, slider=slider, campaign=campaign, tile_ids=tile_ids)

@app.route('/tile')
def show_creative_selection_page():
	"""Shows a page that lets users select the specific creative"""
	
	#get a list of clients for the side bar
	clients = redshift.get_sponsored_client_list(cache)
	
	#get GET variables, if any
	client = request.args.get("client")
	locale = request.args.get("locale")
	
	if client == "Mozilla": #special case since there are so many different types
		
		meta = []
		
		for tile in mozilla_tiles: #mozilla_tiles has already been pre-loaded above
			if 'client' not in tile:
				tiles = []
				for x in tile['ids']:
					tiles.append([x, cache[x]['title'], cache[x]['target_url'], cache[x]['locale'], cache[x]['created_at']])
				tile['tiles'] = sorted(tiles, key=lambda x: x[4], reverse=True) #sort by creative start date, most recent first
				meta.append(
					{
						"name": tile['name'],
						"tiles": len(tile['ids']),
						"locales": len(set([y[3] for y in tile['tiles']])),
						"last_modified": tile['last_modified'],
						"ago": tile['ago']
					}
				)
		
		meta = sorted(meta, key=lambda x: x['last_modified'], reverse=True)
		
		return render_template("index.html", clients=clients, client=client, creative=mozilla_tiles, mozilla=True, mozilla_campaign_meta_data=meta)
	
	else:
		
		#get a list of possible locales and countries
		attributes = redshift.get_client_attributes(cursor, cache, client)
		
		#get a list of all the tiles in that locale
		if locale == "All Locales":
			tiles = redshift.get_tiles_per_client(cache, client)
		else:
			tiles = redshift.get_tiles_from_client_in_locale(cache, client, locale)
	
		if client == 'Yahoo':
			yahoo_filter = redshift.filter_yahoo_tiles(tiles)
			return render_template("index.html", clients=clients, attributes=attributes, client=client, creative=tiles, locale=locale, yahoo_filter=yahoo_filter)
	
		#render the template
		return render_template("index.html", clients=clients, attributes=attributes, client=client, creative=tiles, locale=locale)

@app.route("/countries")
def show_countries():
	"""Shows overall countries"""
	
	#get a list of clients for the side bar
	clients = redshift.get_sponsored_client_list(cache)
	
	#get all possible countries for the dropdown
	countries = redshift.get_all_countries(cache)
	
	#get country impressions data
	country = request.args.get("country")
	country_impressions_data = redshift.get_country_impressions_data(cursor, country=country)
	
	return render_template("countries.html", clients=clients, country_impressions_data=country_impressions_data, country=country, countries=countries)

@app.route("/engagement")
def engagement_testing():
	"""Tests out various methods of engagement"""

	#get a list of clients for the side bar
	clients = redshift.get_sponsored_client_list(cache)

	time_unit = request.args.get('time_unit')
	impressions_data_graph = None
	if time_unit:
		impressions_data = redshift.get_temporal_engagement(cursor, time_unit)
		column_headers = impressions_data[0]
		impressions_data = impressions_data[1:]
		impressions_data_graph = redshift.create_engagement_graph_data(impressions_data, time_unit)
		client = "Overall"
	else:	
		#get some impressions data for a client, or Dashlane by default
		client = request.args.get('client')
		if not client: client = "Dashlane"
		
		column_headers = None
		
		#this new method can pull from a cache
		impressions_data = redshift.get_daily_impressions_data_for_engagement(cursor, client=client)
		
		#add engagement metrics
		impressions_data = redshift.add_engagement_metrics(impressions_data)
	
	return render_template("engagement.html", clients=clients, client=client, impressions_data=impressions_data, column_headers=column_headers, time_unit=time_unit, impressions_data_graph=impressions_data_graph)

@app.route('/get_yahoo_overview_data')
def get_yahoo_overview_data():
	"""Gets yahoo overview data (ajax request)"""
	
	data = {}
	
	#get and format yahoo tiles
	tiles = redshift.get_tiles_per_client(cache, "Yahoo")
	data['yahoo_filter'] = redshift.filter_yahoo_tiles(tiles)
	all_yahoo_tile_ids = []
	for x in data['yahoo_filter']['categories']:
		all_yahoo_tile_ids += x[1].split(',')
	
	#set up query
	query = """
		SELECT tile_id, date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
		FROM impression_stats_daily
		WHERE tile_id in ({0})
		GROUP BY tile_id, date
		ORDER BY date ASC
	""".format(", ".join(all_yahoo_tile_ids))
	
	#grab dataset
	print query
	cursor.execute(query)
	data['dataset'] = []
	
	#clean up dataset
	print "Cleaning dataset"
	for row in cursor.fetchall():
		row = list(row)
		row[1] = "{0}-{1}-{2}".format(row[1].year, row[1].month-1, row[1].day)
		data['dataset'].append(row)
	
	return Response(dumps(data), mimetype='application/json')

@app.route("/overview_interactive")
def overview_interactive():
	"""Testing version of an interactive overview"""
	
	#set up response
	data = {}
	data['clients'] = redshift.get_sponsored_client_list(cache)
	#
	##get and format yahoo tiles
	#tiles = redshift.get_tiles_per_client(cache, "Yahoo")
	#yahoo_filter = redshift.filter_yahoo_tiles(tiles)
	#all_yahoo_tile_ids = []
	#for x in yahoo_filter['categories']:
	#	all_yahoo_tile_ids += x[1].split(',')
	#
	##set up query
	#query = """
	#	SELECT tile_id, date, SUM (impressions) AS impressions, SUM (clicks) AS clicks, SUM (pinned) AS pins, SUM (blocked) AS blocks
	#	FROM impression_stats_daily
	#	WHERE tile_id in ({0})
	#	GROUP BY tile_id, date
	#	ORDER BY date ASC
	#""".format(", ".join(all_yahoo_tile_ids))
	#
	##grab dataset
	#print query
	#cursor.execute(query)
	#data['dataset'] = []
	#
	##clean up dataset
	#print "Cleaning dataset"
	#for row in cursor.fetchall():
	#	row = list(row)
	#	row[1] = "{0}-{1}-{2}".format(row[1].year, row[1].month-1, row[1].day)
	#	data['dataset'].append(row)
	#
	#with open('test_overview_interactive.json', 'w') as f:
	#	print "Dumping dataset"
	#	dump(data['dataset'], f)
	
	#with open('test_overview_interactive.json') as f:
	#	data['dataset'] =  safe_load(f)
	
	return render_template("overview_interactive.html", data=data)

@app.route("/overview")
def overview():
	"""Shows an overview page"""
	
	data = {}
	
	#get a list of clients for the side bar
	data['clients'] = redshift.get_sponsored_client_list(cache)

	#get a list of all possible locales and countries
	data['locales'] = redshift.get_all_locales(cache)
	data['countries'] = redshift.get_all_countries(cache)
	
	#get the parameters
	data['country'] = request.args.get('country')
	data['locale'] = request.args.get('locale')
	data['start_date'] = request.args.get('start_date')
	data['end_date'] = request.args.get('end_date')
	
	#set slider defaults
	#must remember that "bound" means the actual box limits and "values" means the pointer positions.
	slider = { #set defaults
		'start_value': "2014, 8, 12",
		'end_value': "{0}, {1}, {2}".format(datetime.now().year, datetime.now().month-1, datetime.now().day),
		'start_bound': "2014, 8, 12",
		'end_bound': "{0}, {1}, {2}".format(datetime.now().year, datetime.now().month-1, datetime.now().day),
	}
	
	#get the data
	client = request.args.get('client')
	
	if client == 'Yahoo':
		tiles = redshift.get_tiles_per_client(cache, "Yahoo")
		yahoo_filter = redshift.filter_yahoo_tiles(tiles)
		data['impressions'] = redshift.get_yahoo_overview(cursor, yahoo_filter, country=data['country'], locale=data['locale'], start_date=data['start_date'], end_date=data['end_date'])
		data['yahoo'] = True
	else:
		data['impressions'] = redshift.get_overview_data(cursor, mozilla_tiles, cache, country=data['country'], locale=data['locale'], start_date=data['start_date'], end_date=data['end_date'])
	
	#slider parameters
	if data['start_date']: #set possible custom start date
		slider['start_value'] = [int(x) for x in data['start_date'].split("-")]
		slider['start_value'][1] = slider['start_value'][1]-1
		slider['start_value'] = str(slider['start_value'])[1:-1]
	
	if data['end_date']:
		slider['end_value'] = [int(x) for x in data['end_date'].split("-")]
		slider['end_value'][1] = slider['end_value'][1]-1
		slider['end_value'] = str(slider['end_value'])[1:-1]
	
	data['slider'] = slider
	
	return render_template("overview.html", data=data)

@app.route("/projection")
def projection():
	"""Shows the inventory projection dashboard"""
	
	#get a list of clients for the side bar
	clients = redshift.get_sponsored_client_list(cache)

	data = {}

	return render_template("inventory_projection.html", data=data, clients=clients)

@app.route('/')
def show_main_page():
	#get a list of clients for the side bar
	clients = redshift.get_sponsored_client_list(cache)
	#render the template
	return render_template("index.html", clients=clients, main_page=True)

if __name__ == '__main__':
	app.debug = True
	#open_webpage("http://localhost:5000/")
	app.run()
