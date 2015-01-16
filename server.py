#Basic Python Server in Flask for showing reports
#Essentially put together from this page: http://flask.pocoo.org/docs/0.10/quickstart/
#Just run this like:
#> python server.py
#and then visit http://localhost:5000

from flask import Flask, render_template
from redshift import get_client_list, cursor
app = Flask(__name__)

#set up database connection
cursor = cursor()

@app.route('/tile/title/<title>')
def show_specific_tile(title):
	"""A page to show a specific tile's performance"""
	#get a list of clients for the side bar
	clients = get_client_list(cursor)
	#render the template
	return render_template("index.html", clients=clients, title=title)

@app.route('/')
def show_main_page():
	#get a list of clients for the side bar
	clients = get_client_list(cursor)
	#render the template
	return render_template("index.html", clients=clients)

if __name__ == '__main__':
	app.debug = True
	app.run()