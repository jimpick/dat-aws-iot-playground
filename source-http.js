const Dat = require('dat-node')
const pump = require('pump')
const through2 = require('through2')
const port = process.env.PORT || 50000
const IoTStream = require('./iot-stream')
const ram = require('random-access-memory')

const awsIot = require('aws-iot-device-sdk');

const AWS = require('aws-sdk');
AWS.config.region = 'us-east-1';

AWS.config.credentials = new AWS.CognitoIdentityCredentials({
    IdentityPoolId: 'us-east-1:7aadae5c-7106-4616-b5a1-1e24ed4a8b93',
});
const iotEndpoint = 'a1i7z9mmi9v21.iot.us-east-1.amazonaws.com';

let client, iotTopic;

const protocolTypes = {
  2: 'info',
  3: 'have',
  4: 'unhave',
  5: 'want',
  6: 'unwant',
  7: 'request',
  8: 'cancel',
  9: 'data'
}

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
    console.log('AWS IoT connection closed');
};

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
    const opts = {
      key: '72671c5004d3b956791b6ffca7f05025d62309feaf99cde04c6f434189694291',
      sparse: true,
      sparseMetadata: true,
      indexed: false
    }
    // Dat('./local-copy', opts, function (err, dat) {
    Dat(ram, opts, function (err, dat) {
      if (err) throw err

      console.log(`dat://${dat.key.toString('hex')}`)

      const stream = new IoTStream(client, '/serverless/from-src-json')
      client.on('message', (topic, seqMessageBuffer) => {
        if (topic !== '/serverless/from-dest-json') return
        try {
          const seqMessage = JSON.parse(seqMessageBuffer.toString())
          // console.log('Jim received', topic, seqMessage)
          stream.ingest(seqMessage)
        } catch (e) {
          console.error('Exception', e)
        }
      })
      stream.on('end', () => console.log('end'))
      stream.on('finish', () => console.log('finish'))
      stream.on('error', err => console.log('error', err))
      // console.log('Press a key')
      // process.stdin.once('data', function () {
      setTimeout(() => {
        console.log('Pump')

        const replicate = dat.archive.replicate({live: true, encrypt: false})

        replicate.on('handshake', () => {
          console.log(
            'protocol: handshake',
            replicate.remoteId,
            replicate.remoteLive,
            replicate.remoteUserData
          )
        })
        replicate.on('feed', (discoveryKey) => {
          let feedName = 'unknown'
          if (
            discoveryKey.toString('hex') ===
            dat.archive.metadata.discoveryKey.toString('hex')
          ) {
            feedName = 'metadata'
          }
          if (
            dat.archive.content &&
            discoveryKey.toString('hex') ===
            dat.archive.content.discoveryKey.toString('hex')
          ) { 
            feedName = 'content'
          }
          console.log('protocol: feed', feedName)
          // console.log('feeds:', replicate.feeds)
          replicate.feeds.forEach(feed => {
            if (
              discoveryKey.toString('hex') !==
              feed.discoveryKey.toString('hex')
            ) return
            // console.log('Feed matched', feedName, feed)
            const _emit = Object.getPrototypeOf(feed)._emit
            feed._emit = function (type, message) {
              console.log('protocol:', feedName, type,
                protocolTypes[type], message)
              _emit.call(this, type, message)
            }
          })
        })

        pump(
          stream,
					through2(function (chunk, enc, cb) {
						// console.log('s <-- d', chunk)
						this.push(chunk)
						cb()
					}),
          replicate,
          // dat.archive.replicate({encrypt: false}),
					through2(function (chunk, enc, cb) {
						// console.log('s --> d', chunk)
						this.push(chunk)
						cb()
					}),
          stream
        )

        // dat.joinNetwork()
        dat.archive.on('ready', () => console.log('ready'))
        setTimeout(() => {
          console.log('Updating metadata')
          dat.archive.metadata.update(() => {
            console.log('Updated', dat.archive.version)
            dat.archive.readdir('/', (err, files) => {
              if (err) {
                console.error('readdir err', err)
                return
              }
              console.log(files)
              replicate.feeds.forEach(feed => {
                feed.info({
                  downloading: false,
                  uploading: false
                })
              })
              replicate.finalize()
              // replicate.finalize()
              // replicate.destroy()
              // dat.close()
              // replicate.finalize()
              // console.log(replicate)
              // process.exit(0)
            })
            /*
            dat.archive.download('portal.json', (err) => {
              if (err) {
                console.error('download err', err)
                return
              }
              console.log('Downloaded')
              process.exit(0)
            })
            */
          })
        }, 3000)
      }, 5000)

    })

  });

}

run()


