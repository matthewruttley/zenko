#Basic Python Server in Flask for showing reports
#Essentially put together from this page: http://flask.pocoo.org/docs/0.10/quickstart/
#Just run this like:
#> python server.py
#and then visit http://localhost:5000

from webbrowser import open as open_webpage
from flask import Flask, render_template
from redshift import get_sponsored_client_list, cursor, get_tile_locales, get_tile_stats
app = Flask(__name__)

#set up database connection
cursor = cursor()

#@app.route('/tile/title/<title>')
#def show_specific_tile(title):
#	"""A page to show a specific tile's performance"""
#	#get a list of clients for the side bar
#	clients = get_sponsored_client_list(cursor)
#	#get a list of possible locales for these tiles
#	locales = get_tile_locales(cursor, title)
#	#render the template
#	return render_template("index.html", clients=clients, title=title, locales=locales)

@app.route('/tile/<client>/<locale>/<country>/<start_date>')
def show_specific_tile_locale(title, locale):
	"""Grab specific tile data"""
	
	#get a list of clients for the side bar
	clients = get_sponsored_client_list(cursor)
	
	#get a list of possible locales, countries and start dates for these tiles
	attributes = get_tile_attributes(cursor, tile_id)
	
	#get some tile data
	stats = get_tile_stats(cursor, title, locale)
	
	#render the template
	return render_template("index.html", clients=clients, attributes=attributes)

@app.route('/tile/<client>')
def show_selection_page(title, locale):
	"""Shows a page that lets users select the specific locale and country"""
	
	#get a list of clients for the side bar
	clients = get_sponsored_client_list(cursor)
	
	#get a list of possible locales and countries
	attributes = get_tile_attributes(cursor, tile_id)
	
	#render the template
	return render_template("index.html", clients=clients, attributes=attributes)

@app.route('/')
def show_main_page():
	#get a list of clients for the side bar
	clients = get_sponsored_client_list(cursor)
	#render the template
	return render_template("index.html", clients=clients)

if __name__ == '__main__':
	app.debug = True
	#open_webpage("http://localhost:5000/")
	app.run()