//Page functionality
//Mostly things like AJAX requests

function get_creative() {
	//redirects to a page with the correct url for getting the right creative
	var current_tile = document.getElementById('tile_name').innerText
	var locale = document.getElementById('locales').value
	location.href = "/tile/" + current_tile + "/" + locale
}

function filter_creative_by_country() {
	//filters an impressions page by country
	var tile_id = document.getElementById('tile_name').innerText.split("[")[1].split("]")[0]
	var country = document.getElementById('countries').value
	location.href = "/impressions/" + tile_id + "/" + country
}

function filter_impressions_by_date(){
	// Filters impressions using the date slider
	
	start_date = 0
	end_date = 0
	
	
	
}