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

let client, iotTopic;
let buffering = true
const readBuffer = []

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

  /*
  send: (message) => {
      client.publish(iotTopic, message);
  }
  */
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
  constructor () {
    super()
    this.sequence = new Uint32Array(1)
    this.sequence[0] = 0
    // Should probably not use globals
  }

  _write (chunk, encoding, cb) {
    const message = chunk
    console.log('Jim sent', this.sequence[0], typeof message,
      message.length, message)
    this.sequence[0]++;
    const seqMessage = Buffer.concat([
      Buffer.from(this.sequence.buffer),
      message
    ])
    console.log('Jim sent2', seqMessage)
    client.publish('/serverless/from-src', seqMessage, cb)
  }

  _read (size) {
    console.log('Jim _read', size, readBuffer.length)
    buffering = false
    // Do nothing. Use .push() instead
  }
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

    // IoT.send(msg);
    Dat('./data-source', function (err, dat) {
      if (err) throw err

      dat.importFiles()

      console.log(`dat://${dat.key.toString('hex')}`)

      const stream = new IoTStream()
      client.on('message', (topic, seqMessage) => {
        if (topic !== '/serverless/from-dest') return
        console.log('Jim received', topic, typeof seqMessage,
          seqMessage.length, seqMessage)
        if (!buffering) {
          /*
          const pushed = stream.push(seqMessage)
          // stream.resume()
          console.log('Jim pushed', pushed, stream._readableState.flowing)
          if (!pushed) {
            buffering = true
          }
          */
        } else {
          readBuffer.push(seqMessage)
        }
      })
      // console.log('Press a key')
      // process.stdin.once('data', function () {
      setTimeout(() => {
        console.log('Pump')
        pump(
          stream,
					through2(function (chunk, enc, cb) {
						console.log('From dest', typeof chunk, chunk.length, chunk)
						this.push(chunk)
						cb()
					}),
          dat.archive.replicate({live: true, encrypt: false}),
					through2(function (chunk, enc, cb) {
						console.log('From src', typeof chunk, chunk.length, chunk)
						this.push(chunk)
						cb()
					}),
          stream
        )
      }, 5000)
    })

  });

}

run()


