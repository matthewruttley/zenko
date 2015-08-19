//Functionality for Yahoo Category dynamic overview page

function spinner_handler(command){
	//Handles the spinner dialog
	spinner_dialog = document.getElementById('spinner_dialog')
	spinner_table = document.getElementById('spinner_table')
	
	if (command=='start') {
		spinner_dialog.style.visibility = 'visible'
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
		spinner_table.appendChild(cell)
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
			//parse the response
			spinner_handler('Parsing response')
			data = JSON.parse(xmlhttp.responseText)
			
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

function set_up_graphs(data){
	//Activate highcharts graphs
	
	columns = ['impressions', 'clicks', 'ctr', 'pins', 'blocks', 'eng', 'eng_grade']
	graph_data = {}
	
	//convert data into useful format for graphs
	for (var cat of Object.keys(data)) {
		for (var date of Object.keys(data[cat])) {
			for (var x in data[cat][date]) {
				total = data[cat][date][x]
				column_name = columns[x]
				key_name = cat + "_" + column_name
				
				if (graph_data.hasOwnProperty(key_name)===false) {
					graph_data[key_name] = [[date, total]]
				}else{
					graph_data[key_name].push([date, total])
				}
			}
		}
	}
	
	//default impressions payload
	default_impressions = []
	for (var entry of Object.keys(graph_data)) {
		if (entry.indexOf('impressions')!=-1) {
			default_impressions.push({
				'name': entry,
				'yAxis': 0,
				'data': graph_data[entry]
			})
		}
	}
	
	$('#container').highcharts('StockChart', {
		scrollbar: {enabled: true},
		navigator: {enabled: true},
		yAxis: [
					{
						labels: { enabled: false, },
						title: { text: null, },
						min: 0,
					},
				],
		series: default_impressions
	});
}

function fill_table_based_on_date_range(){
	//Fill out each table cell with the correct data, potentially according to a date range
	
}

function elementwise_sum(list1, list2) {
	//Element wise sum of two lists
	//Assumes the lists are of the same length
	sum = []
	for (i=0;i<list1.length;i++) {
		sum.push(list1[i] + list2[i])
	}
	return sum
}

function extract_date(date_string) {
	//converts a string date to a javascript date
	d = date_string.split('-')
	date = Date.UTC(parseInt(d[0]), parseInt(d[1]), parseInt(d[2]))
	return date
}

function insert_metrics(data) {
	//accepts an array like: [impressions, clicks, pins, blocks]
	//returns an array like: [impressions, clicks, ctr, pins, blocks, engagement, engagement_grade]
	
	//insert ctr
	
	if (data[0]===0) {
		ctr = 0
	}else{
		ctr = +((data[1] / data[0]) * 100).toFixed(5) //hack found on SO
	}
	data.splice(2, 0, ctr)
	
	//append engagement
	
	
	
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
	//Process data at the start
	
	//create id to category lookup
	id_to_category = {}
	categories = Object.keys(data.yahoo_filter.categories)
	for (x=0;x<categories.length;x++) {
		category = categories[x]
		tile_ids = data.yahoo_filter.categories[category]
		for (t=0;t<tile_ids.length;t++) {
			id_to_category[tile_ids[t]] = category
		}
	}
	
	//need to aggregate by category
	//also get date range at the same time
	category_aggregate = {}
	date_range = {}
	for (x = 0; x < data.length; x++){
		
		date = extract_date(data[x][1])
		date_range[date] = true
		
		category = id_to_category[data[x][0]]
		
		//make sure category obj is set up correctly
		//and put the data in the object
		if (category_aggregate.hasOwnProperty(category) === false) {
			category_aggregate[category] = {}
		}
		if (category_aggregate[category].hasOwnProperty(date) === false) {
			category_aggregate[category][date] = data[x].slice(2)
		}else{
			category_aggregate[category][date] = elementwise_sum(category_aggregate[category], data[x].slice(2))
		}
	}
	
	//convert date range to an array and sort ascending
	date_range = Object.keys(date_range).sort()
	
	//columns = ['impressions', 'clicks', 'ctr', 'pins', 'blocks', 'eng', 'eng_grade']
	
	//insert CTR, engagement and engagement grade
	for (var category of Object.keys(category_aggregate)){
		for (var date of Object.keys(category_aggregate[category])) {
			data = category_aggregate[category][date]
			data = insert_metrics(data)
			category_aggregate[category][date] = data
		}
	}
	
	return category_aggregate
}
