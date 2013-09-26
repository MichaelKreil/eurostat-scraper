var fs = require('fs');
var request = require('request');
var xml2js = require('xml2js');
var unzip = require('unzip');

get('table_of_contents.xml', function (result) {
	var downloads = result.match(/<nt:downloadLink format=\"sdmx\">.*?<\/nt:downloadLink>/g);
	var urls = {};
	
	downloads.forEach(function (link) {
		var url = link.match(/<nt:downloadLink format=\"sdmx\">http:\/\/epp.eurostat.ec.europa.eu\/NavTree_prod\/everybody\/BulkDownloadListing\?file=(.*?)<\/nt:downloadLink>/);
		url = url[1];
		urls[url] = true;
	});

	Object.keys(urls).forEach(function (url) {
		getSDMX(url, function () {
			console.log(url);
		})
	});
})

function get(file, callback) {
	console.info('Loading '+file);
	var url = 'http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing?file='+file;
	var path = getCacheFilename(file);
	if (fs.existsSync(path)) {
		callback(fs.readFileSync(path, 'binary'));
	} else {
		request({url:url, encoding:'binary'}, function (error, response, body) {
			if (error) console.error(error);
			fs.writeFileSync(path, body, 'binary');
			callback(body);
		})
	}
}

function getXML(file, callback) {
	get(file, function (result) {
		console.info('Parsing '+file);
		xml2js.parseString(result, {trim:true, normalizeTags:true}, function (err, result) {
			if (err) console.log(err);
			callback(result);
		});
	});
}

function getSDMX(file, callback) {
	var path = getCacheFilename(file);
	var pathuz = path+'.unzipped';

	if (fs.existsSync(pathuz)) {
		callback(fs.readFileSync(pathuz, 'binary'));
	} else {
		get(file, function (result) {
			fs.createReadStream(path)
				.pipe(unzip.Extract({ path: pathuz }))
				.on('end', function () {
					callback()
				})
			;
		})
	}
}

function getCacheFilename(file) {
	return '../cache/'+file.replace(/[\\\/\.]/g, '_');
}