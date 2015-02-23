#Basic Python Server in Flask for showing reports
#Essentially put together from this page: http://flask.pocoo.org/docs/0.10/quickstart/
#Just run this like:
#> python server.py
#and then visit http://localhost:5000

from datetime import datetime
from pdb import set_trace
from webbrowser import open as open_webpage
from flask import Flask, render_template, request, make_response
import redshift
app = Flask(__name__)

#set up database connection
cursor = redshift.cursor()
cache = redshift.build_tiles_cache(cursor)
mozilla_tiles = redshift.build_mozilla_tile_list(cache)
#import redshift;cursor=redshift.cursor();cache=redshift.build_tiles_cache(cursor);mozilla_tiles = redshift.build_mozilla_tile_list(cache)

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
	country = request.args.get("country")
	client = request.args.get("client")
	locale = request.args.get("locale")
	campaign = request.args.get("campaign")
	
	#get a list of clients for the side bar
	clients = redshift.get_sponsored_client_list(cache)
	
	#get a list of possible locales and countries
	if client:
		countries = redshift.get_countries_per_client(cache, mozilla_tiles=mozilla_tiles, client=client, campaign=campaign, locale=locale)
	else:
		countries = redshift.get_countries_per_tile(cache, tile_id)
	
	#get some daily data
	if client:
		if client == 'Mozilla':
			if campaign: #special case since mozilla tiles are allowed to have particular campaigns
				tiles = [x['ids'] for x in mozilla_tiles if x['name'] == campaign][0]
				impressions_data = redshift.get_daily_impressions_data(cursor, tile_ids=tiles)
				meta_data = redshift.get_mozilla_meta_data(cache, mozilla_tiles, campaign_name=campaign)
				client = 'Mozilla'
		else:
			if country:
				if locale:
					impressions_data = redshift.get_daily_impressions_data(cursor, client=client, country=country, locale=locale)
				else:
					impressions_data = redshift.get_daily_impressions_data(cursor, client=client, country=country)
			else:
				if locale:
					impressions_data = redshift.get_daily_impressions_data(cursor, client=client, locale=locale)
				else:
					impressions_data = redshift.get_daily_impressions_data(cursor, client=client)
			if locale:
				meta_data = redshift.get_client_meta_data(cache, client=client, locale=locale)
			else:
				meta_data = redshift.get_client_meta_data(cache, client=client)
		specific_tile = False
	else:
		if country:
			impressions_data = redshift.get_daily_impressions_data(cursor, tile_id=tile_id, country=country)
		else:
			impressions_data = redshift.get_daily_impressions_data(cursor, tile_id=tile_id)
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
	return render_template("index.html", clients=clients, client=client, meta_data=meta_data, countries=countries, impressions_data=impressions_data, country=country, tile_id=tile_id, locale=locale, specific_tile=specific_tile, impressions_data_graph=graph, campaign=campaign, error=error)

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
		meta_data = redshift.get_tile_meta_data(cache, tile_id)
		client = [x[1] for x in meta_data if x[0] == 'title'][0]
		specific_tile = tile_id
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
		if type(start_bound) == unicode:
			start_bound = datetime.strptime(start_bound, "%Y-%m-%d %H:%M:%S.%f")
		slider = {
			'start_bound': "{0}, {1}, {2}".format(start_bound.year, start_bound.month-1, start_bound.day),
			'end_bound': "{0}, {1}, {2}".format(end_bound.year, end_bound.month-1, end_bound.day),
			'start_value': "{0}, {1}, {2}".format(start_bound.year, start_bound.month-1, start_bound.day),
			'end_value': "{0}, {1}, {2}".format(end_bound.year, end_bound.month-1, end_bound.day),
		}
	
	#render the template
	return render_template("index.html", clients=clients, client=client, meta_data=meta_data, countries_impressions_data=countries_impressions_data, slider=slider, specific_tile=specific_tile, locale=locale, campaign=campaign)

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
		meta_data = redshift.get_tile_meta_data(cache, tile_id)
		client = [x[1] for x in meta_data if x[0] == 'title'][0]
		specific_tile = tile_id
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
		if type(start_bound) == unicode:
			start_bound = datetime.strptime(start_bound, "%Y-%m-%d %H:%M:%S.%f")
		slider = {
			'start_bound': "{0}, {1}, {2}".format(start_bound.year, start_bound.month-1, start_bound.day),
			'end_bound': "{0}, {1}, {2}".format(end_bound.year, end_bound.month-1, end_bound.day),
			'start_value': "{0}, {1}, {2}".format(start_bound.year, start_bound.month-1, start_bound.day),
			'end_value': "{0}, {1}, {2}".format(end_bound.year, end_bound.month-1, end_bound.day),
		}
	
	return render_template("index.html", client=client, clients=clients, start_date=start_date, countries=countries, end_date=end_date, country=country, tile_id=tile_id, specific_tile=tile_id, locale_impressions_data=impressions_data, slider=slider, campaign=campaign)

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
						"locales": len(set([y[3] for y in tile['tiles']]))
					}
				)
		
		return render_template("index.html", clients=clients, client=client, creative=mozilla_tiles, mozilla=True, mozilla_campaign_meta_data=meta)
		
	else:
		#get a list of possible locales and countries
		attributes = redshift.get_client_attributes(cursor, cache, client)
		
		#get a list of all the tiles in that locale
		if locale == "All Locales":
			tiles = redshift.get_tiles_per_client(cache, client)
		else:
			tiles = redshift.get_tiles_from_client_in_locale(cache, client, locale)
	
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

	#get some impressions data for a client, or Dashlane by default
	client = request.args.get('client')
	if not client: client = "Dashlane"
	#this new method can pull from a cache
	impressions_data = redshift.get_daily_impressions_data_for_engagement(cursor, client=client)
	
	#add engagement metrics
	impressions_data = redshift.add_engagement_metrics(impressions_data)
	
	return render_template("engagement.html", clients=clients, client=client, impressions_data=impressions_data)

@app.route("/overview")
def overview():
	"""Shows an overview page"""
	
	#get a list of clients for the side bar
	clients = redshift.get_sponsored_client_list(cache)

	#get a list of all possible locales and countries
	locales = redshift.get_all_locales(cache)
	countries = redshift.get_all_countries(cache)
	
	#get the parameters
	country = request.args.get('country')
	locale = request.args.get('locale')
	start_date = request.args.get('start_date')
	end_date = request.args.get('end_date')
	
	#get the data
	data = {}
	data['impressions'] = redshift.get_overview_data(cursor, mozilla_tiles, cache, country=country, locale=locale, start_date=start_date, end_date=end_date)
	
	#slider parameters
	#must remember that "bound" means the actual box limits and "values" means the pointer positions.
	slider = { #set defaults
		'start_value': "2014, 8, 12",
		'end_value': "{0}, {1}, {2}".format(datetime.now().year, datetime.now().month-1, datetime.now().day),
		'start_bound': "2014, 8, 12",
		'end_bound': "{0}, {1}, {2}".format(datetime.now().year, datetime.now().month-1, datetime.now().day),
	}
	
	if start_date: #set possible custom start date
		slider['start_value'] = [int(x) for x in start_date.split("-")]
		slider['start_value'][1] = slider['start_value'][1]-1
		slider['start_value'] = str(slider['start_value'])[1:-1]
	
	if end_date:
		slider['end_value'] = [int(x) for x in end_date.split("-")]
		slider['end_value'][1] = slider['end_value'][1]-1
		slider['end_value'] = str(slider['end_value'])[1:-1]
	
	data['slider'] = slider
	
	return render_template("overview.html", data=data, clients=clients, country=country, locale=locale, countries=countries, locales=locales)

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

