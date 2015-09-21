#Basic Python Server in Flask for showing reports
#Essentially put together from this page: http://flask.pocoo.org/docs/0.10/quickstart/
#Just run this like:
#> python server.py
#and then visit http://localhost:5000

from json import dump, dumps
from datetime import datetime
from pdb import set_trace
from collections import defaultdict

from webbrowser import open as open_webpage
from flask import Flask, render_template, request, make_response, Response, jsonify
import redshift

app = Flask(__name__)
cache = redshift.build_tiles_cache() #set up database connection

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

@app.route('/impressions')
def show_impressions():
	"""Shows daily impressions for a tile_id/client"""
	
	data = {}
	
	#get cached data
	data['clients'] = redshift.get_tile_data(cache, attributes=['client', 'status'], sort='client')
	data['countries'] = sorted(cache['countries']['country_to_code'].keys())
	
	#get pivot
	data['pivot'] = request.args.get('pivot')
	
	#parse selectors
	data['selectors'] = redshift.parse_selectors(request.args)
	
	#get impressions data
	data['impressions'] = redshift.get_impressions_data(cache, data['pivot'], data['selectors'])
	
	#get other page settings
	data['meta_data'] = redshift.get_meta_data(cache, data['selectors'])
	data['slider'] = get_slider_parameters(data['meta_data'], data['selectors'])
	
	#convert the data to be graph-able
	if data['pivot'] == 'date':
		data['graph'] = redshift.convert_impressions_data_for_graph(data['impressions'])
		data['summary'] = ["Impressions", "Clicks", "CTR", "Blocks", "Pins"]
	
	#insert an error message if needed
	data['error'] = False
	if len(data['impressions']) == 0:
		data['error'] = error()
	
	data['url'] = make_url_from_selectors('impressions', data['selectors']) #build the url
	data['impressions_title'] = make_impressions_page_title(data['selectors']) #make the title
	
	#render the template
	return render_template("impressions.html", data=data)

@app.route('/selection')
def show_creative_selection_page():
	"""Shows a page that lets users select the specific creative"""
	
	#page elements
	data = {}
	data['page_title'] = 'Creative Selection Page'
	data['clients'] = redshift.get_tile_data(cache, attributes=['client', 'status'], sort='client')
	
	#data
	data['client'] = request.args.get('client')
	campaigns = redshift.get_tile_data(cache, selectors={'client':data['client']}, attributes='campaign', sort='campaign')
	tiles = redshift.get_tile_data(cache, selectors={'client':data['client']})
	
	#organize the tiles by campaign
	data['campaigns'] = {}
	for campaign in campaigns: #create mapping object
		if campaign['campaign']:
			data['campaigns'][campaign['campaign']] = []
	data['campaigns']["Uncategorized"] = [] #add general bucket as well
	
	#insert the data
	for tile in tiles:
		if tile['campaign'] == False:
			data['campaigns']["Uncategorized"].append(tile)
		else:
			data['campaigns'][tile['campaign']].append(tile)
	
	#meta-data about the campaigns (different from overall selection meta-data)
	data['metadata'] = defaultdict(dict)
	for campaign, tiles in data['campaigns'].iteritems():
		data['metadata'][campaign]['tile_count'] = 0
		data['metadata'][campaign]['locale_count'] = set()
		data['metadata'][campaign]['last_modified'] = datetime(1900, 1, 1)
		for tile in tiles:
			data['metadata'][campaign]['tile_count'] += 1
			data['metadata'][campaign]['locale_count'].update(tile['locale'])
			if tile['created_at'] > data['metadata'][campaign]['last_modified']:
				data['metadata'][campaign]['last_modified'] = tile['created_at']
		data['metadata'][campaign]['locale_count'] = len(data['metadata'][campaign]['locale_count'])
	
	#remove Uncategorized if empty
	if data['campaigns']['Uncategorized'] == []:
		del data['campaigns']['Uncategorized']
		del data['metadata']['Uncategorized']
	
	return render_template('selection.html', data=data)

@app.route('/')
def show_main_page():
	data = {}
	data['page_title'] = ''
	data['clients'] = redshift.get_tile_data(cache, attributes=['client', 'status'], sort='client')
	return render_template("selection.html", data=data)

@app.route('/refresh_cache')
def refresh_cache():
	"""Refreshes the cache"""
	
	cache = redshift.build_tiles_cache(force_redownload=True)
	return jsonify({'message':"Cache refreshed!"})
	
#Extras, auxiliary and custom filters

@app.template_filter('thousands')
def add_commas(number):
	"""1234567890 ---> 1,234,567,890"""
	return "{:,}".format(number)

def error():
	"""Gets an error message"""
	
	return {
			"message": ["The Redshift Tiles server doesn't seem to be returning any data for this analysis.", "Sound odd? Please contact "],
			"contact": "mruttley@mozilla.com"
		}

def make_url_from_selectors(page, selectors):
	"""Makes a url for a page from selectors"""
	
	url_components = []
	
	for k, v in selectors.iteritems():
		if k != 'pivot':
			if type(v) in [set, list]:
				v = ",".join([unicode(x) for x in v])
			url_components.append(u"{0}={1}".format(k, v))
	
	return u"/" + page + u"?" + u"&".join(url_components)

def make_impressions_page_title(selectors):
	"""Makes a nicely formatted title for the impressions page"""
	
	impressions_title = []
	
	fields = ['client', 'campaign', 'id']
	for field in fields:
		if field in selectors:
			l = list(selectors[field])
			if len(l) == 1:
				impressions_title.append(unicode(l[0]))
			else:
				impressions_title.append("{0} {1}s".format(len(l), field))
	
	if impressions_title == []:
		impressions_title = "Impressions Data: Overview"
	else:
		impressions_title = "Impressions Data: " + " - ".join(impressions_title)
	
	return impressions_title

def get_slider_parameters(meta_data, selectors):
	"""Calculate the bounds for the sliders.
	This needs an overall start + end and also a value start+end
	"Bound" refers to the actual full overall slider
	"Value" refers to the arbitrary setting
	"""
	
	slider = {}
	
	#set bounds
	#start bound is either the earliest date of the meta-data or 2014-08-30
	slider['start_bound'] = meta_data['created_at'].strftime("%Y, {0}, %d".format(meta_data['created_at'].month-1))
	
	#set end bound to today
	slider['end_bound'] = datetime.now().strftime("%Y, {0}, %d".format(datetime.now().month-1))
	
	#check if pointers were specified before in selectors
	#they will be in the format u"2015-08-01" if so
	for position in ['start', 'end']:
		if position + '_date' in selectors:
			value = [int(x) for x in selectors[position + '_date'].split("-")]
			slider[position + '_value'] = "{0}, {1}, {2}".format(value[0], value[1]-1, value[2])
		else:
			slider[position + '_value'] = slider[position + '_bound']
	
	return slider

if __name__ == '__main__':
	app.debug = True
	#open_webpage("http://localhost:5000/")
	app.run()
