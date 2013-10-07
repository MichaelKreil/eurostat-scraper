var fs = require('fs');
var request = require('request');
var xml2js = require('xml2js');
var AdmZip = require('adm-zip');
var path = require('path');
var async = require('async');

var config = JSON.parse(fs.readFileSync('./config.json', 'utf8'));


getCached(config.cacheFolder+'table_of_contents.xml', 'http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing?sort=1&file=table_of_contents.xml', function (result) {
	result = result.toString('utf8');
	result = result.replace(/[\s\t\r\n]+/g, ' ');
	var datasets = result.match(/<nt:leaf type=\"dataset\">.*?<\/nt:leaf>/g);
	var codes = {};

	function findReg(text, regexp) {
		var result = text.match(regexp);
		if (result == null) return false;
		return result[1];
	}
	
	datasets.forEach(function (dataset) {
		var code = dataset.match(/<nt:code>(.*?)<\/nt:code>/)[1];
		codes[code] = {
			code:      code,
			
			update:    findReg(dataset, /<nt:lastUpdate>(.*?)<\/nt:lastUpdate>/),
			meta_html: findReg(dataset, /<nt:metadata format=\"html\">(.*?)<\/nt:metadata>/),
			meta_sdmx: findReg(dataset, /<nt:metadata format=\"sdmx\">(.*?)<\/nt:metadata>/),
			tsv:       findReg(dataset, /<nt:downloadLink format=\"tsv\">(.*?)<\/nt:downloadLink>/),
			dft:       findReg(dataset, /<nt:downloadLink format=\"dft\">(.*?)<\/nt:downloadLink>/),
			sdmx:      findReg(dataset, /<nt:downloadLink format=\"sdmx\">(.*?)<\/nt:downloadLink>/)//"
		}
	});

	datasets = Object.keys(codes).map(function (code) { return codes[code] });

	async.eachLimit(datasets, 4,
		function (dataset, callback) {
			files = [];
			if (dataset.meta_html) {
				files.push({
					local:config.resultFolder+'meta_html/'+dataset.code+'.html',
					url:dataset.meta_html
				});
			}
			if (dataset.meta_sdmx) {
				files.push({
					local:config.cacheFolder+'meta_sdmx/'+dataset.code+'.sdmx.zip',
					url:dataset.meta_sdmx
				});
			}
			if (dataset.tsv) {
				files.push({
					local:config.cacheFolder+'tsv/'+dataset.code+'.tsv.gz',
					url:dataset.tsv
				});
			}
			if (dataset.dft) {
				files.push({
					local:config.cacheFolder+'dft/'+dataset.code+'.dft.gz',
					url:dataset.dft
				});
			}
			if (dataset.sdmx) {
				files.push({
					local:config.cacheFolder+'sdmx/'+dataset.code+'.sdmx.zip',
					url:dataset.sdmx
				});
			}

			async.eachSeries(files, function (file, callback) {
				getCached(file.local, file.url, function () {
					callback();
				});
			}, function () {
				callback()
			})
		},
		function (err) {
			console.error('Error', err);
		}
	);
})

function get(url, callback) {

	request({url:url, encoding:null}, function (error, response, body) {
		if (error) console.error(error);
		callback(body);
	});

	return;
	/*

	var path = getCacheFilename(url);
	if (fs.existsSync(path)) {
		console.info('Loading '+url);
		setTimeout(function () {
			callback(fs.readFileSync(path));
		}, 0);
	} else {
		console.info('Downloading '+url);
		request({url:url, encoding:null}, function (error, response, body) {
			if (error) console.error(error);
			fs.writeFileSync(path, body);
			callback(body);
		})
	}*/
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

function getSDMX(file, callback, dontLoad) {
	var basename = file.replace(/^.*\//g, '').replace(/\.sdmx.*$/g, '');
	var pathdata = getResultFilename(basename+'.sdmx.xml', 'sdmx');
	var pathmeta = getResultFilename(basename+'.dsd.xml',  'sdmx');

	if (fs.existsSync(pathdata) && fs.existsSync(pathmeta)) {
		console.log('SDMX-Loading '+file);

		setTimeout(function () {
			if (dontLoad) {
				callback();
			} else {
				callback({
					data:fs.readFileSync(pathdata),
					meta:fs.readFileSync(pathmeta)
				});
			}
		}, 0);
	} else {
		get(file, function (data) {
			console.log('SDMX-Unzipping '+file);
			var zip = new AdmZip(data);
			var result = {};

			zip.getEntries().forEach(function (entry) {
				if (entry.header.size < (1 << 30)) {
					var data = entry.getData();
					switch (entry.name) {
						case basename + '.dsd.xml':
							fs.writeFileSync(pathmeta, data);
							result.meta = data;
						break;
						case basename + '.sdmx.xml':
							fs.writeFileSync(pathdata, data);
							result.data = data;
						break;
					}
				}
			});

			if (dontLoad) {
				callback();
			} else {
				callback({
					data:fs.readFileSync(pathdata),
					meta:fs.readFileSync(pathmeta)
				});
			}
		})
	}
}


function getCached(file, url, callback, dontLoad) {
	ensureFolder(file);

	if (fs.existsSync(file)) {
		console.log('File-Loading '+file);

		setTimeout(function () {
			if (dontLoad) {
				callback();
			} else {
				callback(fs.readFileSync(file));
			}
		}, 0);
	} else {
		console.log('File-Downloading '+file);
		get(url, function (data) {
			fs.writeFileSync(file, data)
			if (dontLoad) {
				callback();
			} else {
				callback(data);
			}
		})
	}
}

function getCacheFilename(file, subfolder) {
	var filename = config.cacheFolder + 'download/' + (subfolder ? subfolder+'/' : '') + file.replace(/[\\\/\.]/g, '_');
	ensureFolder(filename);
	return filename;
}

function getResultFilename(file, subfolder) {
	var filename = config.resultFolder + (subfolder ? subfolder+'/' : '') + file;
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
