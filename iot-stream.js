const { Duplex } = require('stream')

class IoTStream extends Duplex {
  constructor (client, publishTopic, options) {
    super(options)
    // Should probably not use globals
    this.client = client
    this.publishTopic = publishTopic

    // For writing
    this.sequence = 0

    // For reading
    this.buffering = true
    this.readBuffer = {}
    this.pushedSeq = 0
    this.maxQueuedSeq = 0
    this.on('finish', () => {
      this.finished = true
      this.client.end()
    })
  }

  _write (chunk, encoding, cb) {
    const message = chunk
    // console.log('Jim sent', this.sequence[0], typeof message,
    //   message.length, message)
    this.sequence++;
    const seqMessage = JSON.stringify({
      sequence: this.sequence,
      message: message.toString('base64')
    })
    // console.log('Jim sent2', this.publishTopic, seqMessage)
    // this.client.publish('/serverless/from-src-json', seqMessage, cb)
    this.client.publish(this.publishTopic, seqMessage, cb)
  }

  _read (size) {
    // Do nothing. Use .push() instead
    // console.log('Jim _read', size, Object.keys(this.readBuffer).length)
    this.buffering = false
    this.processQueue()
  }

  ingest (seqMessage) {
    const seq = seqMessage.sequence
    const message = Buffer.from(seqMessage.message, 'base64')
    // console.log('Jim queued', seq, message)
    this.readBuffer[seq] = message
    this.maxQueuedSeq = Math.max(this.maxQueuedSeq, seq)
    this.processQueue()
  }

  processQueue () {
    // console.log('Jim start processQueue', this.pushedSeq, this.maxQueuedSeq, this.processing, this.buffering)
    if (this.processing) return
    this.processing = true
    if (this.buffering) {
      this.processing = false
      return
    }
    while (this.pushedSeq < this.maxQueuedSeq) {
      const nextSeq = this.pushedSeq + 1
      const message = this.readBuffer[nextSeq]
      // console.log('Jim seq message', nextSeq, message, this.readBuffer)
      if (!message) {
        this.processing = false
        return
      }
      if (this.finished) {
        this.processing = false
        return
      }
      const pushed = this.push(message)
      delete this.readBuffer[nextSeq]
      this.pushedSeq = nextSeq
      // console.log('Jim pushed', nextSeq, this.maxQueuedSeq, pushed,
      //  this._readableState.flowing)
      if (!pushed) {
        this.buffering = true
        this.processing = false
        return
      }
    }
    this.processing = false
  }
}

module.exports = IoTStream

