var fs = require('fs');
var request = require('request');
var xml2js = require('xml2js');
var AdmZip = require('adm-zip');
var path = require('path');
var async = require('async');

get('table_of_contents.xml', function (result) {
	result = result.toString('utf8');
	var downloads = result.match(/<nt:downloadLink format=\"sdmx\">.*?<\/nt:downloadLink>/g);
	var urls = {};
	
	downloads.forEach(function (link) {
		var url = link.match(/<nt:downloadLink format=\"sdmx\">http:\/\/epp.eurostat.ec.europa.eu\/NavTree_prod\/everybody\/BulkDownloadListing\?file=(.*?)<\/nt:downloadLink>/);
		url = url[1];
		urls[url] = true;
	});

	var urlList = Object.keys(urls).map(function (url) { return url });

	async.eachLimit(urlList, 4,
		function (url, callback) {
			getSDMX(url, function (sdmx) {
				callback();
			})
		},
		function (err) {
			console.error('Error', err);
		}
	);
})

function get(file, callback) {
	var url = 'http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing?file='+file;
	var path = getCacheFilename(file, 'download');
	if (fs.existsSync(path)) {
		console.info('Loading '+file);
		setTimeout(function () {
			callback(fs.readFileSync(path));
		}, 0);
	} else {
		console.info('Downloading '+file);
		request({url:url, encoding:null}, function (error, response, body) {
			if (error) console.error(error);
			fs.writeFileSync(path, body);
			callback(body);
		})
	}
}

function getXML(file, callback) {
	get(file, function (result) {
		console.info('XML-Parsing '+file);
		result = result.toString('utf8');
		xml2js.parseString(result, {trim:true, normalizeTags:true}, function (err, result) {
			if (err) console.log(err);
			callback(result);
		});
	});
}

function getSDMX(file, callback) {
	var path = getCacheFilename(file, 'sdmx');
	var pathdata = path+'.data';
	var pathmeta = path+'.meta';

	if (fs.existsSync(pathdata) && fs.existsSync(pathmeta)) {
		console.log('SDMX-Loading '+file);

		setTimeout(function () {
			callback({
				//data:fs.readFileSync(pathdata),
				//meta:fs.readFileSync(pathmeta)
			});
		}, 0);
	} else {
		get(file, function (data) {
			var zip = new AdmZip(data);
			var basename = file.replace(/^.*\//g, '').replace(/\.sdmx.*$/g, '');
			var result = {};

			zip.getEntries().forEach(function (entry) {
				var data = entry.getData();
				switch (entry.name) {
					case basename +  '.dsd.xml':
						fs.writeFileSync(pathmeta, data);
						result.meta = data;
					break;
					case basename + '.sdmx.xml':
						fs.writeFileSync(pathdata, data);
						result.data = data;
					break;
				}
			});

			callback(result);
		})
	}
}

function getCacheFilename(file, subfolder) {
	var filename = '../cache/' + (subfolder ? subfolder+'/' : '') + file.replace(/[\\\/\.]/g, '_');
	ensureFolder(filename);
	return filename;
}

function ensureFolder(folder) {
	folder = path.resolve(path.dirname(require.main.filename), folder);
	var rec = function (fol) {
		if (fol != '/') {
			rec(path.dirname(fol));
			if (!fs.existsSync(fol)) fs.mkdirSync(fol);
		}
	}
	rec(path.dirname(folder));
}
