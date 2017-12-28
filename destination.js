const { Duplex } = require('stream')
const Dat = require('dat-node')
const pump = require('pump')
const through2 = require('through2')
const port = process.env.PORT || 50000

const awsIot = require('aws-iot-device-sdk');

const AWS = require('aws-sdk');
AWS.config.region = 'us-east-1';

AWS.config.credentials = new AWS.CognitoIdentityCredentials({
    IdentityPoolId: 'us-east-1:7aadae5c-7106-4616-b5a1-1e24ed4a8b93',
});
const iotEndpoint = 'a1i7z9mmi9v21.iot.us-east-1.amazonaws.com';
const key = '96ab03a6aeca21f53182d78ad9672f0313ee26dd10462d1ab9c27c48d8d7a82d'

let client, iotTopic;

const IoT = {
  connect: (topic, iotEndpoint, region, accessKey, secretKey, sessionToken) => {

    iotTopic = topic;

    client = awsIot.device({
      region: region,
      protocol: 'wss',
      accessKeyId: accessKey,
      secretKey: secretKey,
      sessionToken: sessionToken,
      port: 443,
      host: iotEndpoint
    });

    client.on('connect', onConnect);
    // client.on('message', onMessage);
    client.on('error', onError);
    client.on('reconnect', onReconnect);
    client.on('offline', onOffline);
    client.on('close', onClose);
  },

  send: (message) => {
      client.publish(iotTopic, message);
  }
};

const onConnect = () => {
    client.subscribe(iotTopic);
    console.log('Connected');
};

const onMessage = (topic, message) => {
    console.log(message.toString());
};

const onError = () => {};
const onReconnect = () => {};
const onOffline = () => {};

const onClose = () => {
    console.log('Connection failed');
};

class IoTStream extends Duplex {
  constructor (options) {
    super(options)
    // Should probably not use globals

    // For writing
    this.sequence = new Uint32Array(1)
    this.sequence[0] = 0

    // For reading
    this.buffering = true
    this.readBuffer = {}
    this.pushedSeq = 0
    this.maxQueuedSeq = 0
  }

  _write (chunk, encoding, cb) {
    const message = chunk
    // console.log('Jim sent', this.sequence[0], typeof message,
    //   message.length, message)
    this.sequence[0]++;
    const seqMessage = Buffer.concat([
      Buffer.from(this.sequence.buffer),
      message
    ])
    // console.log('Jim sent2', seqMessage)
    client.publish('/serverless/from-dest', seqMessage, cb)
  }

  _read (size) {
    // Do nothing. Use .push() instead
    // console.log('Jim _read', size, Object.keys(this.readBuffer).length)
    this.buffering = false
    this.processQueue()
  }

  ingest (seqMessage) {
    const seq = seqMessage.readUInt32LE(0) // Is it always LE?
    const message = seqMessage.slice(4)
    // console.log('Jim queued', seq, message)
    this.readBuffer[seq] = message
    this.maxQueuedSeq = Math.max(this.maxQueuedSeq, seq)
    this.processQueue()
  }

  processQueue () {
    // console.log('start processQueue', this.pushedSeq, this.maxQueuedSeq, this.processing)
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

const oldEnd = IoTStream.prototype.end
IoTStream.prototype.end = function () {
  // console.trace('Jim end', oldEnd)
  // console.log('Jim end')
  oldEnd.apply(this, Array.from(arguments))
}

function run () {
  let iotKeys;

  AWS.config.credentials.get(() => {
    // Use these credentials with IoT
    const accessKey = AWS.config.credentials.accessKeyId;
    const secretKey = AWS.config.credentials.secretAccessKey;
    const sessionToken = AWS.config.credentials.sessionToken;
    iotKeys = {
        iotEndpoint,
        region: AWS.config.region,
        accessKey,
        secretKey,
        sessionToken
    }
    console.log(`Endpoint: ${iotKeys.iotEndpoint},
            Region: ${iotKeys.region},
            AccessKey: ${iotKeys.accessKey},
            SecretKey: ${iotKeys.secretKey},
            SessionToken: ${iotKeys.sessionToken}`);

    const iotTopic = '/serverless/#';

    IoT.connect(iotTopic,
                iotKeys.iotEndpoint,
                iotKeys.region,
                iotKeys.accessKey,
                iotKeys.secretKey,
                iotKeys.sessionToken);

    Dat('./data-destination', {key}, function (err, dat) {
      if (err) throw err

      console.log(`dat://${dat.key.toString('hex')}`)

      // const stream = new IoTStream({ objectMode: true })
      const stream = new IoTStream()
      client.on('message', (topic, seqMessage) => {
        if (topic !== '/serverless/from-src') return
        // console.log('Jim received', topic, typeof seqMessage,
        //  seqMessage.length, seqMessage)
        stream.ingest(seqMessage)
      })
      stream.on('end', () => console.log('end'))
      stream.on('finish', () => console.log('finish'))
      stream.on('error', err => console.log('error', err))
      // console.log('Press a key')
      // process.stdin.once('data', function () {
      setTimeout(() => {
        console.log('Pump')
        const replicate = dat.archive.replicate({live: true, encrypt: false})
        replicate.on('end', () => console.log('replicate end'))
        replicate.on('finish', () => console.log('replicate finish'))
        replicate.on('error', err => console.log('replicate error', err))
        pump(
          stream,
					through2(function (chunk, enc, cb) {
						console.log('s --> d', chunk)
						this.push(chunk)
						cb()
					}),
          replicate,
					through2(function (chunk, enc, cb) {
						console.log('s <-- d', chunk)
						this.push(chunk)
						cb()
					}),
          stream
        )
        dat.archive.on('update', () => {
          console.log('update', dat.archive.version)
        })
      }, 5000)
    })
  })
}

run()
