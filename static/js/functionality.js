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

function filter_lbl_by_country(){
	//filters locale-by-locale page by country and/or date slider
	
	//get start and end
	var chosen_values = $("#slider").dateRangeSlider("values")
	start_date = [chosen_values.min.getFullYear(), chosen_values.min.getMonth()+1, chosen_values.min.getDate()].join("-")
	end_date = [chosen_values.max.getFullYear(), chosen_values.max.getMonth()+1, chosen_values.max.getDate()].join("-")
	
	if (location.href.indexOf('client=')!=-1) { //particular client
		var tile_name = document.getElementById('tile_name').textContent
		redirect = "/locale_impressions?client=" + tile_name + "&start_date=" + start_date + "&end_date=" + end_date
	}else{ //particular tile
		var tile_id = document.getElementById('tile_id').textContent
		redirect = "/locale_impressions?tile_id=" + tile_id + "&start_date=" + start_date + "&end_date=" + end_date
	}
	
	country = document.getElementById("countries").value
	if (country != "All Countries") {
		redirect += "&country=" + country
	}
	
	location.href = redirect
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

function convert_table_to_array() {
	//convert the current table to a list of lists
	itable = document.getElementById("impressions_table")
	
	//convert the table to a list of lists
	var data = [];
	
	//meta data
	col_count = itable.children[0].children[0].children.length
	row_count = itable.children[1].children.length
	
	//grab the header
	header_cells = itable.children[0].children[0].children
	header = []
	for (i=0;i<header_cells.length;i++) {
		header.push(header_cells[i].textContent)
	}
	data.push(header.join("#"))
	
	//get each row
	row_cells = itable.children[1].children
	
	for (i=0;i<row_cells.length;i++) {
		row = itable.children[1].children[i].children
		
		//get each cell in the row
		row_content = []
		for (j=0;j<row.length;j++) {
			cell_content = row[j].textContent//.replace(",", "")
			row_content.push(cell_content)
		}
		data.push(row_content.join("#"))
	}
	return data.join("###")
}

function download_xls() {
	//Downloads the current table as an excel file
	
	//Works by:
	//1. Creating an iframe
	//2. Inserting a form with a hidden field
	//3. Putting the table data into the hidden field
	//4. Submitting via post the form data and filename
	//5. The server then converts that to excel and redirects to the page
	//6. It appears as a file download
	
	iframe = document.createElement("iframe")
	iframe.setAttribute("width", 1)
	iframe.setAttribute("height", 1)
	iframe.setAttribute("frameborder", 0)
	iframe.setAttribute("src", "about:blank")
	
	form = document.createElement("form")
	form.setAttribute("method", "POST")
	form.setAttribute("action", "/download_excel")
	
	data = document.createElement("input")
	data.setAttribute("type", "hidden")
	data.setAttribute("value", convert_table_to_array())
	data.setAttribute("name", "data")
	
	filename = document.createElement("input")
	filename.setAttribute("type", "hidden")
	filename.setAttribute("value", location.href)
	filename.setAttribute("name", "filename")
	
	form.appendChild(data)
	form.appendChild(filename)
	iframe.appendChild(form)
	document.body.appendChild(iframe)
	
	form.submit()
}

function filter_country_overview_by_country() {
	country = document.getElementById("countries").value
	if (country == "All Countries") {
		location.href = "/countries"
	}else{
		location.href = "/countries?country=" + country
	}
}

function seriesExists(series_name){
	//Does a series exist on the chart?
	var series = $("#container").highcharts().series
	for (i=0;i<series.length;i++) {
		if (series[i].name == series_name) {
			return true
		}
	}
	return false
}

function set_checkbox_label_colors() {
	var series = $("#container").highcharts().series
	for (i=0;i<series.length;i++) {
		if (series[i].name != "Navigator") {
			label_name = series[i].name
			console.log("looking up " + label_name)
			document.getElementById(label_name).parentElement.style.color = series[i].color
		}
	}
}

function filter_engagement(){
	//functionality for client filter
	client = document.getElementById('clients').value
	if (client == "All Clients") {
		location.href = "/engagement"
	}else{
		location.href = "/engagement?client=" + client
	}
}

function filter_overview() {
	//filters the overview page
	locale = document.getElementById('locales').value
	country = document.getElementById('countries').value
	parameters = []
	if (locale!='All Locales') {
		parameters.push("locale="+locale)
	}
	if (country!='All Countries') {
		parameters.push("country="+country)
	}
	var chosen_values = $("#slider").dateRangeSlider("values")
	start_date = [chosen_values.min.getFullYear(), chosen_values.min.getMonth()+1, chosen_values.min.getDate()].join("-")
	end_date = [chosen_values.max.getFullYear(), chosen_values.max.getMonth()+1, chosen_values.max.getDate()].join("-")
	parameters.push("start_date="+start_date)
	parameters.push("end_date="+end_date)
	
	parameters = parameters.join("&")
	redirect = "/overview?" + parameters
	
	location.href = redirect
}


























