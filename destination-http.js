const dnscache = require('dnscache')({
	"enable" : true,
	"ttl" : 300,
	"cachesize" : 1000
});
const hyperdrive = require('hyperdrive')
const pump = require('pump')
const through2 = require('through2')
const port = process.env.PORT || 50000
const IoTStream = require('./iot-stream')
const datHttp = require('dat-http')

const awsIot = require('aws-iot-device-sdk');

const AWS = require('aws-sdk');
AWS.config.region = 'us-east-1';

AWS.config.credentials = new AWS.CognitoIdentityCredentials({
    IdentityPoolId: 'us-east-1:7aadae5c-7106-4616-b5a1-1e24ed4a8b93',
});
const iotEndpoint = 'a1i7z9mmi9v21.iot.us-east-1.amazonaws.com';
const key = '72671c5004d3b956791b6ffca7f05025d62309feaf99cde04c6f434189694291'

var storage = datHttp('https://s3-us-west-2.amazonaws.com/dat-backups/rotonde-jimpick/')

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

		const httpDrive = hyperdrive(storage, key, {
			latest: true,
			live: false
		})
		httpDrive.on('ready', () => {
			console.log('Jim ready', httpDrive.version)
			console.log('Jim discoveryKey', httpDrive.discoveryKey)
			httpDrive.metadata.update(() => {
				console.log('Jim metadata update')
			})    

      const stream = new IoTStream(client, '/serverless/from-dest-json')
      client.on('message', (topic, seqMessageBuffer) => {
        if (topic !== '/serverless/from-src-json') return
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
        const replicate = httpDrive.replicate({live: true, encrypt: false})
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
        httpDrive.on('update', () => {
          console.log('update', httpDrive.version)
        })
      }, 5000)
    })
  })
}

run()
