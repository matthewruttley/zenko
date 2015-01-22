//Page functionality
//Mostly things like AJAX requests

function get_creative() {
	//redirects to a page with the correct url for getting the right creative
	
	var current_tile = document.getElementById('tile_name').innerText //for some reason basic location.href stuff isn't working
	var locale = document.getElementById('locales').value
	var redirect = "/" + current_tile + "/" + locale
	console.log("Redirecting to " + redirect)
	window.location.href = redirect
}