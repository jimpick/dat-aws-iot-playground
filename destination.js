const Dat = require('dat-node')
const wss = require('websocket-stream')
const pump = require('pump')
const port = process.env.PORT || 50000

const key = '96ab03a6aeca21f53182d78ad9672f0313ee26dd10462d1ab9c27c48d8d7a82d'

Dat('./data-destination', {key}, function (err, dat) {
  if (err) throw err

  console.log(`dat://${dat.key.toString('hex')}`)

  const stream = wss(`ws://localhost:${port}`)
  pump(
    stream,
    dat.archive.replicate({live: true, encrypt: false}),
    stream
  )
  dat.archive.on('update', () => {
    console.log('update', dat.archive.version)
  })
})
