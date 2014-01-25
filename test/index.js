var backup = require('../')
var assert = require('assert')
var fs = require('fs')
var knox = require('knox')

var i = Math.round(Math.random() * 100)
var dest = 'test' + i + '.zip'

var s3opts = {
  "key": "",
  "secret": "",
  "bucket": "backuper-test"
}
var s3 = knox.createClient(s3opts)

backup.pack({
  format: 'zip',
  src: '*.couch',
  cwd: __dirname + '/data/',
  dest: dest
}, function (err, path, bytes) {
  assert.ifError(err)
  assert.equal(bytes, 246)
  console.log('#pack() OK')
  
  backup.upload(s3, path, bytes, function (err, res) {
    assert.ifError(err)
    assert.equal(res.statusCode, 200)
    
    fs.open(path, 'r', function (err) {
      assert.equal(err.code, 'ENOENT')
      console.log('#upload() OK')
      
      setTimeout(function () {
        backup.clean(s3, '1s', function (err, data) {
          assert.ifError(err)
          assert(Array.isArray(data))
          assert.equal(data.length, 1)
          console.log('#clean() OK')
        })
      }, 2000)
    })
  })
})