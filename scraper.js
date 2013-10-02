var fs = require('fs');
var request = require('request');
var xml2js = require('xml2js');
var unzip = require('unzip');
var path = require('path');
var async = require('async');
var Stream = require('stream');
var Buffer = require('buffer');
var Bufferstream = require('bufferstream');

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

	async.eachLimit(urlList, 2,
		function (url, callback) {
			getSDMX(url, function (sdmx) {
				callback();
			})
		},
		function (err) {
			console.log(err);
		}
	);
})

function get(file, callback) {
	var url = 'http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing?file='+file;
	var path = getCacheFilename(file, 'download');
	if (fs.existsSync(path)) {
		console.info('Loading '+file);
		callback(fs.readFileSync(path));
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
		callback({
			data:fs.readFileSync(pathdata),
			meta:fs.readFileSync(pathmeta)
		});
	} else {
		get(file, function (data) {
			var unzipper = unzip.Parse();
			var result = {};
			
			var basename = file.replace(/^.*\//g, '').replace(/\.sdmx.*$/g, '');
			console.log('SDMX-Unzipping '+file);

			unzipper.on('entry', function (entry) {
				var stream, filename;
				switch (entry.path) {
					case basename +  '.dsd.xml': filename = pathmeta; stream = result.meta = new Bufferstream({encoding:'utf8', size:'flexible'}); break;
					case basename + '.sdmx.xml': filename = pathdata; stream = result.data = new Bufferstream({encoding:'utf8', size:'flexible'}); break;
				}
				if (stream) {
					stream.on('close', function () {
						fs.writeFileSync(filename, stream.getBuffer());
					});
					entry.pipe(stream);
				}
			});


			var stream = new Stream.Readable();
			stream._read = function () {
				stream.push(data);
				data = null;
			}

			unzipper.on('close', function () {
				if (result.meta && !result.meta.finished) result.meta.end();
				if (result.data && !result.data.finished) result.data.end();
				callback(result);
			});
			
			stream.pipe(unzipper);
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
