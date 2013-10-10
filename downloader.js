var FS = require('fs');
var Path = require('path');
var Url = require('url');
var Request = require('request');
var Async = require('async');

var remotePath = 'http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing?start=all&dir=';
var downloadPath = 'http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing?downfile=';
var resultPath = '../dump/';
var cachedPath = '../cache/';

var downloadList = [];
var visitedDirs = {};


// Scanne das Verzeichnis und lade alle runter
scan('', downloadAll);


// Lade alle gefundenen Dateien runter.
function downloadAll() {
	Async.eachLimit(
		downloadList,
		4, // Lade bis zu 4 Dateien gleichzeitig
		function (file, callback) {
			var overwrite = true;
			var remoteFile = downloadPath+file.download.replace(/\//g, '%2F');
			var resultFile = resultPath+file.download;

			// Wenn die Datei schon existiert und neuer ist, als auf dem Server,
			// dann überschreibe die lokale Version nicht.
			if (FS.existsSync(resultFile)) {
				var stat = FS.statSync(resultFile);
				if (file.time < stat.mtime) overwrite = false;
			}

			if (overwrite) {
				console.log('Downloading: '+file.download);
				
				ensureFolder(resultFile);

				var stream = Request(remoteFile);
				stream.pipe(FS.createWriteStream(resultFile))
				stream.on('end', callback);
			} else {
				setTimeout(callback, 0);
			}
		},
		function (err) {
			if (err) console.error(err);
			console.log('Finished')
		}
	)
}


// Scanne die Verzeichnisse rekursiv
function scan(url, callback, suffix) {
	var  localFile = cachedPath + url + '/index';
	var remoteFile = remotePath + url.replace(/\//g, '%2F');

	if (suffix) {
		 localFile += suffix.replace(/[=\?\&]/g, '_');
		remoteFile += suffix;
	}

	localFile += '.html';


	if (visitedDirs[remoteFile]) {
		callback();
		return
	}
	visitedDirs[remoteFile] = true;
	


	getCached(
		localFile,
		remoteFile,
		function (body) {
			scanPage(
				body.toString().replace(/[\r\t\n]/g, ''),
				callback
			);
		}
	)
}

// Durchsuche eine index-Seite nach Links
function scanPage(body, callback) {
	var filetable = body.match(/class=\"filelist\"(.*?)<\/table>/)[1];
	var files = filetable.match(/<tr .*?<\/tr>/g).map(function (row) {
		var cells = row.match(/<td.*?<\/td>/g);
		cells = cells.map(function (cell) {
			var result = { text: cell.replace(/<.*?>/g, '').replace(/&nbsp;/g, ' ').replace(/^\s+|\s+$/g, '') };

			var href = cell.match(/href=\"(.*?)\"/);
			if (href) {
				var url = href[1].replace(/&amp;/g, '&');
				result.url = url;
				result.query = Url.parse(url, true).query;
			}

			return result;
		});

		var result = {
			title: cells[0].text,
			url:   cells[0].url,
			size:  cells[1].text,
			isDir: cells[2].text == 'DIR',
			date:  cells[3].text,
		};

		var d = result.date;
		d = d.substr(3,2)+'/'+d.substr(0,2)+'/'+d.substr(6);
		result.time = new Date(d);

		if (cells[0].query) {
			result.dir = cells[0].query.dir && cells[0].query.dir.replace(/%2F/gi, '/');
		}

		if (cells[4].query) result.download = cells[4].query.downfile;

		return result;
	});

	Async.eachSeries(
		files,
		function (file, callback) {
			if (file.title.indexOf('up one level') >= 0) {
				// ignorieren
				callback();
				return;
			}

			if (file.isDir) {
				if ('_comp_comext_dic_metadata'.indexOf('_'+file.dir) >= 0) {
					// ignorieren
					callback();
					return;
				}

				if (file.dir == 'data') {
					Async.eachSeries(
						['DFT', 'TSV', 'SDMX'],
						function (filter, callback) {
							scan(file.dir, callback, '&filter='+filter);
						},
						function (err) {
							if (err) console.error(err);
							callback();
						}
					);
				} else {
					scan(file.dir, callback);
				}
			} else {
				downloadList.push(file);
				setTimeout(callback,0);
			}
		},
		function (err) {
			if (err) console.error(err);
			callback();
		}
	)
}





function get(url, callback) {
	Request({url:url, encoding:null}, function (error, response, body) {
		if (error) console.error(error);
		callback(body);
	});
	return;
}

function getCached(file, url, callback) {
	if (FS.existsSync(file)) {

		console.log('File-Loading '+file);

		setTimeout(function () {
			callback(FS.readFileSync(file));
		}, 0);

	} else {

		ensureFolder(file);
		console.log('File-Downloading '+url);
		get(url, function (data) {
			FS.writeFileSync(file, data)
			callback(data);
		})

	}
}

function ensureFolder(folder) {
	folder = Path.resolve(Path.dirname(require.main.filename), folder);

	var rec = function (fol) {
		if (fol != '/') {
			rec(Path.dirname(fol));
			if (!FS.existsSync(fol)) FS.mkdirSync(fol);
		}
	}

	rec(Path.dirname(folder));
}
