#Basic Python Server in Flask for showing reports
#Essentially put together from this page: http://flask.pocoo.org/docs/0.10/quickstart/
#Just run this like:
#> python server.py
#and then visit http://localhost:5000

from flask import Flask, render_template
from redshift import get_sponsored_client_list, cursor, get_tile_locales, get_tile_stats
app = Flask(__name__)

#set up database connection
cursor = cursor()

@app.route('/tile/title/<title>')
def show_specific_tile(title):
	"""A page to show a specific tile's performance"""
	#get a list of clients for the side bar
	clients = get_sponsored_client_list(cursor)
	#get a list of possible locales for these tiles
	locales = get_tile_locales(cursor, title)
	#render the template
	return render_template("index.html", clients=clients, title=title, locales=locales)

@app.route('/tile_locale/<title>/<locale>')
def show_specific_tile_locale(title, locale):
	"""Show only one specific locale for that tile"""
	#get a list of clients for the side bar
	clients = get_sponsored_client_list(cursor)
	#get a list of possible locales for these tiles
	locales = get_tile_locales(cursor, title)
	#get some tile data
	stats = get_tile_stats(cursor, title, locale)
	print "got {0} days".format(stats)
	#render the template
	return render_template("index.html", clients=clients, title=title, locales=locales, days=stats)

@app.route('/')
def show_main_page():
	#get a list of clients for the side bar
	clients = get_sponsored_client_list(cursor)
	#render the template
	return render_template("index.html", clients=clients)

if __name__ == '__main__':
	app.debug = True
	app.run()