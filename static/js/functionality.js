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
		if (location.href.indexOf('client=Mozilla')!=-1) {
			redirect = "/locale_impressions?client=Mozilla&start_date=" + start_date + "&end_date=" + end_date
			//get possible country
			if (location.href.indexOf('country=')!=-1) { //kind of hackish, could be cleaned up in the future
				country = location.href.split('country=')[1].split('&')[0]
				redirect += "&country=" + country
			}
		}else{
			var tile_name = document.getElementById('tile_name').textContent
			redirect = "/locale_impressions?client=" + tile_name + "&start_date=" + start_date + "&end_date=" + end_date
		}
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
	// for the country-by-country analysis
	
	//get start and end
	var chosen_values = $("#slider").dateRangeSlider("values")
	start_date = [chosen_values.min.getFullYear(), chosen_values.min.getMonth()+1, chosen_values.min.getDate()].join("-")
	end_date = [chosen_values.max.getFullYear(), chosen_values.max.getMonth()+1, chosen_values.max.getDate()].join("-")
	
	if (location.href.indexOf('client=')!=-1) { //particular client
		if (location.href.indexOf('client=Mozilla')!=-1) {
			redirect = "/country_impressions?client=Mozilla&start_date=" + start_date + "&end_date=" + end_date
			//get possible locale
			if (location.href.indexOf('locale=')!=-1) { //kind of hackish, could be cleaned up in the future
				locale = location.href.split('locale=')[1].split('&')[0]
				redirect += "&locale=" +locale
			}
		}else{
			var tile_name = document.getElementById('tile_name').textContent
			if (location.href.indexOf('locale=')!=-1) { //does it include locale as well?
				var locale = document.getElementById("locale").textContent
				redirect = "/country_impressions?client=" + tile_name + "&start_date=" + start_date + "&end_date=" + end_date + "&locale=" + locale
			}else{
				redirect = "/country_impressions?client=" + tile_name + "&start_date=" + start_date + "&end_date=" + end_date
			}
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
		
		//check that the row is visible (could have been hidden)
		if (row_cells[i].style.display == 'none') {
			continue
		}
		
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
	if ('client=Yahoo' in location.href) {
		parameters.push("client=Yahoo")
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

function setSummaryBoxValues(min, max) {
	//sets the values of the summary box
	
	for (var key in impressions_data) {
		
		if ((key=='Engagement') || (key=='EGrade')) {
			continue
		}
		
		//get the total 
		total = 0
		count = 0
		for (i=0;i<impressions_data[key].length;i++) {
			if (min==0) { //if all data was requested using 0,0
				total += impressions_data[key][i][1]
				count += 1
			}else{
				//specific range
				if (impressions_data[key][i][0] >= min) {
					if (impressions_data[key][i][0] <= max) {
						total += impressions_data[key][i][1]
						count += 1
					}else{
						break
					}
				}
			}
		}
		
		//locate where to place the data
		summary = document.getElementById(key.toLowerCase() + "_summary")
		average = document.getElementById(key.toLowerCase() + "_average")
		
		if (key == 'CTR') {
			//special case
			impressions_summary = parseInt(document.getElementById('impressions_summary').innerHTML.replace(/\,/g, ""))
			clicks_summary = parseInt(document.getElementById("clicks_summary").innerHTML.replace(/\,/g, ""))
			summary.innerHTML = ((clicks_summary/impressions_summary)*100).toFixed(4) +"%"
			average.innerHTML =  "n/a" // (total/count).toFixed(4) + "%" //this doesn't mean anything
		}else{
			summary.innerHTML = total.toString().split(".")[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",")
			average.innerHTML = (total/count).toFixed(4).toString().split(".")[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",")
		}
	}
}

function showHideRows(min, max){
	//Shows or hides rows based on the slider
	
	min = min.split(" ")[0].split("-") //get the 2015-01-01 part of the date
	min = new Date(min[0], min[1], min[2])
	max = max.split(" ")[0].split("-")
	max = new Date(max[0], max[1], max[2])
	
	//now iterate through all rows
	rows = document.getElementById("impressions_table").children[1].children
	hide = true
	for (r=0;r<rows.length;r++) {
		
		date = rows[r].children[0].textContent.split("-")
		date = new Date(date[0], date[1], date[2])
		
		if ((date > min) && (date < max)) {
			rows[r].style.display = "table-row" //show
		}else{
			rows[r].style.display = "none" //hide
		}
	}
}

function baseline_chart(){
	//sets the yaxis extremes
	ya = $("#container").highcharts().series[0].yAxis
	button = document.getElementById('baseline')
	if (ya.min == 700) {
		ya.setExtremes(0, 1000)
		button.innerHTML = "Baseline chart<br>at 700"
	}else{
		ya.setExtremes(700, 1000)
		button.innerHTML = "Baseline chart<br>at 0"
	}
}

function what_tiles_have_been_selected(){
	//Finds out which checkboxes on the page have been checked
	checkboxes = document.getElementsByClassName('multicheck')
	ids = []
	for (var x in checkboxes) {
		if (checkboxes[x].checked===true) {
			id = checkboxes[x].id.split('select_')[1]
			ids.push(id)
		}
	}
	return ids
}

function multiple_tile_selection(tile_id) {
	//Multiple tiles have been selected
	//adjust the page accordingly
	tiles = what_tiles_have_been_selected()
	dialog = document.getElementById("floating_multi_tile_select")
	if (tiles.length > 1) {
		//show the container
		dialog.style.visibility = "visible"
		area = document.getElementById('which_tiles_selected')
		area.innerHTML = tiles.join(", ")
	}else{
		dialog.style.visibility = "hidden"
	}
}

function multiple_tile_show(){
	//shows multiple tiles
	tiles = what_tiles_have_been_selected()
	location.href = '/daily_impressions?tile_ids=' + tiles.join(',')
}

function launch_yahoo_filter() {
	//constructs a url and redirects to it
	
	tile_ids = get_tiles_selected()
	tile_ids = tile_ids.join(",")
	
	location.href = "/daily_impressions?tile_ids=" + tile_ids
	
}

function get_tiles_selected() {
	//gets the ids from the tiles that have been selected in the yahoo filters
	
	flights = document.getElementById('yahoo_flight_dates').value
	cats = document.getElementById('yahoo_categories').value
	
	if ((flights.indexOf('All')!=-1) && (cats.indexOf('All')!=-1)) { //neither
		return "all"
	}
	
	if ((flights.indexOf('All')==-1) && (cats.indexOf('All')==-1)) { //both
		//find intersection
		flights = flights.split(',')
		cats = cats.split(',')
		intersection = []
		console.log('got cats and flight, cats len ' + cats.length + " flight len " + flights.length)
		for (var flight of flights) {
			if (cats.indexOf(flight) != -1) {
				intersection.push(flight)
			}
		}
		console.log('intersection len ' + intersection.length)
		return intersection
	}
	
	if (flights.indexOf('All')==-1) { //just flights
		flights = flights.split(',')
		return flights
	}
	
	if (cats.indexOf('All')==-1) { //just cats
		cats = cats.split(',')
		return cats
	}
	
	console.log('error in yahoo intersection')
	return "error" //something went wrong, so just return nothing
}

function filter_yahoo(){
	//filters yahoo tiles based on the contents of the select elements
	
	//first get lists of tiles
	tile_ids = get_tiles_selected()
	
	if (tile_ids == "all") {
		everything = true
		document.getElementById("yahoo_filter_button").style.visibility = 'hidden'
	}else{
		everything = false
		document.getElementById("yahoo_filter_button").style.visibility = 'visible'
		document.getElementById("num_yahoo_tiles").innerHTML = tile_ids.length
	}
	
	table = document.getElementById('tile_list')
	rows = table.children[1].children //tbody child elements
	
	for (var x = 0; x < rows.length; x++) {
		row = rows[x]
		tile_id = row.children[1].children[0].textContent //second td containing id
		if (everything) {
			row.style.display = 'table-row'	
		}else{
			if (tile_ids.indexOf(tile_id) != -1) {
				row.style.display = 'table-row'	
			}else{
				row.style.display = 'none'
			}
		}
	}
	
}







