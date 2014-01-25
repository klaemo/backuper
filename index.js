var archiver = require('archiver')
var knox = require('knox')
var fs = require('fs')
var path = require('path')
var moment = require('moment')

module.exports = function backup (opts, done) {
  var s3 = knox.createClient(opts.s3)
  
  pack(opts, function (err, path, bytes) {
    if (err) return done(err)
    
    upload(s3, path, bytes, function (err, res) {
      if (err) return done(err)
    
      clean(s3, opts.retention, function (err, data) {
        if (err) return done(err)
        done()
      })
    })
  })
}

module.exports.pack = pack
module.exports.upload = upload
module.exports.clean = clean

function pack (opts, cb) {
  var output = fs.createWriteStream(opts.dest)
  var archive = archiver(opts.format || 'zip')

  output.on('close', function () {
    cb(null, output.path, output.bytesWritten)
  })

  archive.on('error', onError)
  output.on('error', onError)

  function onError(err) { cb(err) }

  archive.pipe(output)

  archive.bulk([
    { expand: true, cwd: opts.cwd, src: opts.src }
  ])

  archive.finalize(onError)
}

function upload (s3, archive, bytes, cb) {
  var fileName = path.basename(archive)
  var req = s3.put(fileName, {
    'Content-Length': bytes,
    'Content-Type': 'application/zip'
  })

  fs.createReadStream(archive).pipe(req)

  req.on('response', function (res) {
    del(archive, function () {
      cb(null, res)
    })
  })

  req.on('error', function (err) {
    del(archive, function () {
      cb(err)
    })
  })
}

function del (file, cb) {
  fs.unlink(file, function (err) {
    if (err) return console.error(err)
    cb()
  })
}

function clean (s3, ret, cb) {
  s3.list(function (err, data) {
    if (err) return cb(err)

    var retention = moment().subtract(ret.match(/\D+/)[0], ret.match(/^\d+/)[0])

    var old = data.Contents.filter(function (file) {
      return moment(file.LastModified).isBefore(retention)
    }).map(function (file) {
      return file.Key
    })

    if (!old.length) return cb(null, [])

    s3.deleteMultiple(old, function (err, res) {
      if (err) return cb(err)
      cb(null, old)
    })
  })
}

