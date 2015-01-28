//Page functionality
//Mostly things like AJAX requests

function get_creative() {
	//redirects to a page with the correct url for getting the right creative
	var client_name = document.getElementById('tile_name').textContent
	var locale = document.getElementById('locales').value
	location.href = "/tile?client=" + client_name + "&locale=" + locale
}

function filter_creative_by_country() {
	//filters an impressions page by country
	var country = document.getElementById('countries').value
	if (location.href.indexOf('client=')!=-1) { //particular client
		var tile_name = document.getElementById('tile_name').textContent
		if (location.href.indexOf('locale=')!=-1) { //does it include locale as well?
			var locale = document.getElementById("locale").textContent
			location.href = "/daily_impressions?client=" + tile_name + "&country=" + country + "&locale=" + locale
		}else{
			location.href = "/daily_impressions?client=" + tile_name + "&country=" + country
		}
	}else{
		var tile_id = document.getElementById('tile_id').textContent
		location.href = "/daily_impressions?tile_id=" + tile_id + "&country=" + country
	}
}

function filter_impressions_by_date(){
	// Filters impressions using the date slider
	
	//get start and end
	var chosen_values = $("#slider").dateRangeSlider("values")
	start_date = [chosen_values.min.getFullYear(), chosen_values.min.getMonth()+1, chosen_values.min.getDate()].join("-")
	end_date = [chosen_values.max.getFullYear(), chosen_values.max.getMonth()+1, chosen_values.max.getDate()].join("-")
	
	if (location.href.indexOf('client=')!=-1) { //particular client
		var tile_name = document.getElementById('tile_name').textContent
		if (location.href.indexOf('locale=')!=-1) { //does it include locale as well?
			var locale = document.getElementById("locale").textContent
			redirect = "/country_impressions?client=" + tile_name + "&start_date=" + start_date + "&end_date=" + end_date + "&locale=" + locale
		}else{
			redirect = "/country_impressions?client=" + tile_name + "&start_date=" + start_date + "&end_date=" + end_date
		}
	}else{ //particular tile
		var tile_id = document.getElementById('tile_id').textContent
		redirect = "/country_impressions?tile_id=" + tile_id + "&start_date=" + start_date + "&end_date=" + end_date
	}
	location.href = redirect
}

function show_all_client_data(){
	//redirects to the day-by-day page
	var client_name = document.getElementById("tile_name").textContent
	var locale = document.getElementById("locales").value
	if (locale == "All Locales") {
		location.href = "/daily_impressions?client=" + client_name
	}else{
		location.href = "/daily_impressions?client=" + client_name + "&locale=" + locale
	}
}