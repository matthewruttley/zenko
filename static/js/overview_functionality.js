//Functionality for Yahoo Category dynamic overview page

//SETTINGS

var original_columns = ['impressions', 'clicks', 'pins', 'blocks']
var columns_with_metrics = ['impressions', 'clicks', 'ctr', 'pins', 'blocks', 'eng', 'eng_grade']
//var data = false
//var yahoo_filter = false

//FUNCTIONALITY

function spinner_handler(command){
	//Handles the spinner dialog
	spinner_dialog = document.getElementById('spinner_dialog')
	spinner_table = document.getElementById('spinner_table')
	
	if (command=='start') {
		spinner_dialog.style.visibility = 'visible'
		
		//remove any existing rows in the spinner table
		for (var c of spinner_table.childNodes) {
			spinner_table.removeChild(c)
		}
		
	}
	else if (command=='end') {
		spinner_dialog.style.visibility = 'hidden'
	}
	else {
		//create new row with a cell
		cell = document.createElement('td')
		row = document.createElement('tr')
		row.appendChild(cell)
		
		//add text
		text = document.createTextNode(command + "...")
		cell.appendChild(text)
		
		//add to table
		spinner_table.appendChild(row)
	}
}

function get_data_from_server() {
	//start spinner dialog
	spinner_handler('start')
	spinner_handler('Getting data from Redshift')
	
	//Gets data from the server
	var xmlhttp=new XMLHttpRequest();
	xmlhttp.onreadystatechange=function()
	{
		if (xmlhttp.readyState==4 && xmlhttp.status==200)
		{
			spinner_handler('Parsing response')
			response = JSON.parse(xmlhttp.responseText)
			data = response.dataset
			yahoo_filter = response.yahoo_filter
			
			//process data
			spinner_handler('Formatting data')
			data = process_data(data)
			
			//activate graphs
			spinner_handler('Setting up graphs')
			set_up_graphs(data)
			
			//fill table
			spinner_handler('Filling out tables')
			fill_table_based_on_date_range(data)
			
			//close spinner
			spinner_handler('end')
		}
	}
	xmlhttp.open("GET","/get_yahoo_overview_data", true);
	xmlhttp.send();
}

function setup_page(data) {
	//start the spinner if not already started
	spinner_handler('start')
	
	//process data
	spinner_handler('Formatting data')
	data = process_data(data)
	
	//activate graphs
	spinner_handler('Setting up graphs')
	set_up_graphs(data)
	
	//fill table
	spinner_handler('Filling out tables')
	fill_table_based_on_date_range(data)
	
	//close spinner
	spinner_handler('end')
}

function set_up_graphs(data){
	//Activate highcharts graphs
	
	graph_data = {}
	
	//convert data into useful format for graphs
	//currently in the format:
	//	{
	//		'auto_gen___imps': {
	//			'029384701298374': 432
	//		}
	//	}
	//and we need it in the format:
	//	{
	//		'auto_gen___imps': [
	//			['029384701298374', 432]
	//		]
	//	}
	
	for (var cat of Object.keys(data)) {
		tmp_list = []
		for (var date of Object.keys(data[cat])) {
			tmp_list.push([parseInt(date), data[cat][date]])
		}
		graph_data[cat] = tmp_list
	}
	
	//default impressions payload
	default_impressions = []
	for (var entry of Object.keys(graph_data)) {
			default_impressions.push({
				'name': entry,
				'yAxis': 0,
				'data': graph_data[entry]
			})
	}
	
	$('#category_graph_container').highcharts('StockChart', {
		scrollbar: {enabled: true},
		navigator: {enabled: true},
		yAxis: [
					{
						labels: { enabled: false, },
						title: { text: null, },
						min: 0,
					},
				],
		xAxis: {
			events: {
				setExtremes: function(e) {
					
					ed = extract_date(Highcharts.dateFormat(null, e.min).toString().split()[0], reduce=true)
					ld = extract_date(Highcharts.dateFormat(null, e.max).toString().split()[0], reduce=true)
					
					fill_table_based_on_date_range(
						data,
						earliest_date=ed,
						latest_date=ld
					)
				}
			}
		},
		series: default_impressions
	});
}

function set_up_category_rows(data, earliest_date=false, latest_date=false) {
	
	//set + check parameters
	if (earliest_date == false) {
		earliest_date = Date.UTC(2014,8,1)
	}
	if (latest_date == false) {
		latest_date = Date.now()
	}
	
	//aggregate the data according to the date range
	category_rows = {} 
	for (category of Object.keys(data)) {
		//sum up the totals for each column
		dates = Object.keys(data[category])
		
		category_split = category.split('___')
		category_name = category_split[0]
		column_name = category_split[1]
		column_index = original_columns.indexOf(column_name)
		
		if (category_rows.hasOwnProperty(category_name) == false) {
			category_rows[category_name] = []
			for (var x in original_columns) {
				category_rows[category_name].push(0)
			}
		}
		
		for (date of dates){
			date_int = parseInt(date)
			if ((date_int <= latest_date) && (date_int >= earliest_date)) {
				if (data[category].hasOwnProperty(date)) {
					category_rows[category_name][column_index] += data[category][date]
				}
			}
		}
	}
	
	//now set up ctr and engagement+grade
	
	for (category of Object.keys(category_rows)) {
		
		//get the data
		row = category_rows[category]
		
		//calculate metrics
		clickthrough = ctr(row[0], row[1])
		eng = engagement(row[1], row[3])
		eng_grade = engagement_grade(eng)
		
		//insert
		row.splice(2, 0, clickthrough)
		row.push(eng, eng_grade)
		
		//replace
		category_rows[category] = row
	}
	
	return category_rows
}

function fill_table_based_on_date_range(data, earliest_date=false, latest_date=false){
	//Fill out each table cell with the correct data, potentially according to a date range
	
	data_rows = set_up_category_rows(data, earliest_date=earliest_date, latest_date=latest_date)
	
	category_table_container = document.getElementById('category_table_container')
	if (category_table_container.children.length != 0) { //remove the table if it exists
		category_table_container.removeChild(category_table_container.childNodes[0])
	}
	
	table = document.createElement('table')
	table.className = "table table-responsive table-striped table-hover sortable"
	category_table_container.appendChild(table)
	
	//header row
	thead = document.createElement('thead')
	header_row = document.createElement('tr')
	header_cols = columns_with_metrics
	if (header_cols[0] != 'Category') {
		header_cols.unshift('Category') //prepends the string
	}
	for (column of header_cols) {
		cell = document.createElement('th')
		//have to capitalize the column name
		column_name = column.charAt(0).toUpperCase() + column.slice(1)
		text = document.createTextNode(column_name.replace('_', ' '))
		cell.appendChild(text)
		header_row.appendChild(cell)
	}
	thead.appendChild(header_row)
	table.appendChild(thead)
	
	//body
	body = document.createElement('tbody')
	table.appendChild(body)
	
	//get the column sums for each category
	//data is in the format: {category:{date:[0,0,0,0,0,0]}}
	
	sorted_category_rows = Object.keys(data_rows)
	sorted_category_rows.sort()
	
	for (category of sorted_category_rows) {
		
		//set up category row
		category_row = document.createElement('tr')
		
		//add the name
		category_name_cell = document.createElement('td')
		category_name_text = document.createTextNode(category)
		category_name_cell.appendChild(category_name_text)
		category_row.appendChild(category_name_cell)
		
		//add the data
		for (entry of data_rows[category]) {
			cell = document.createElement('td')
			text = document.createTextNode(entry)
			cell.appendChild(text)
			category_row.appendChild(cell)
		}
		
		body.appendChild(category_row)
	}
	
	//Refresh the sortable-ness
	$.bootstrapSortable()
}

function extract_date(date_string, reduce=false) {
	//converts a string date to a javascript date
	d = date_string.split('-')
	
	//months are 0-11
	if (reduce) {
		month = parseInt(d[1]) - 1
	}else{
		month = parseInt(d[1])
	}
	
	date = Date.UTC(parseInt(d[0]), month, parseInt(d[2]))
	return date
}

function ctr(impressions, clicks) {
	//calculate and return ctr
	if (impressions===0) {
		return 0
	}else{
		return +((clicks / impressions) * 100).toFixed(5)
	}
}

function engagement_grade(engagement){
	//Scores the engagement with a letter
	
	over = [
		[997, "A++"],
		[992, "A+"],
		[979, "A"],
		[971, "B++"],
		[960, "B+"],
		[951, "B"],
		[933, "C++"],
		[898, "C+"],
		[0, "C"]
	]
	
	for (var score of over) {
		if (engagement >= score[0]) {
			return score[1]
		}
	}
	
	return "?"
}

function engagement(blocks, clicks){
	//Adds m7 engagement
	
	if (clicks == 0) {
		return 0
	}
	
	e = blocks / clicks
	e = e * 10
	e = 1000 - e
	e = Math.round(e)
	
	return e
}

function process_data(data){
	//Processes data at the start
	//	Starting format:
	//	data = [[2281, "2015-8-3", 7498, 50, 1, 116], [2279, "2015-8-3", 3320, 22, 3, 98], ...]
	//
	//	Ending format:
	//	data = {
	//		'cat1___field': {
	//			'date1': [1,2,3,4,5,6,7],
	//			'date2': [1,2,3,4,5,6,7],
	//		}
	//	}
	
	//create id to category lookup
	id_to_category = {}
	categories = []
	for (entry of yahoo_filter.categories) {
		categories.push(entry[0])
		for (tile_id of entry[1].split(',')) {
			id_to_category[tile_id] = entry[0]
		}
	}
	
	//need to aggregate by category
	//also get date range at the same time
	category_aggregate = {}
	date_range = {}
	for (entry of data){
		
		//extract date
		date = extract_date(entry[1])
		date_range[date] = true
		
		//iterate through columns
		totals = entry.slice(2)
		for (i in totals) {
			column_name = original_columns[i]
			total = totals[i]
			
			//setup name
			category = id_to_category[entry[0]]
			category_header = category + "___" + column_name //concatenated for highcharts later on
			
			//make sure category obj is set up correctly
			//and put the data in the object
			if (category_aggregate.hasOwnProperty(category_header) === false) {
				category_aggregate[category_header] = {}
			}
			if (category_aggregate[category_header].hasOwnProperty(date) === false) {
				category_aggregate[category_header][date] = total
			}else{
				category_aggregate[category_header][date] += total
			}
		
		}
	}
	
	//convert date range to an array and sort ascending
	date_range = Object.keys(date_range).sort()
	
	return category_aggregate
}
