const http = require('http')
const Dat = require('dat-node')
const wss = require('websocket-stream')
const pump = require('pump')
const port = process.env.PORT || 50000

Dat('./data-source', function (err, dat) {
  if (err) throw err

  dat.importFiles()

  console.log(`dat://${dat.key.toString('hex')}`)
  const server = http.createServer()
  wss.createServer({
    server,
    perMessageDeflate: false
  }, stream => {
    pump(
      stream,
      dat.archive.replicate({live: true, encrypt: false}),
      stream
    )
  })
  server.listen(port, function () {
    console.log(
      'WebSocket server listening on port %d',
      server.address().port
    )
  })
})
