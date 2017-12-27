const { Duplex } = require('stream')
const Dat = require('dat-node')
const pump = require('pump')
const port = process.env.PORT || 50000

const awsIot = require('aws-iot-device-sdk');

const AWS = require('aws-sdk');
AWS.config.region = 'us-east-1';

AWS.config.credentials = new AWS.CognitoIdentityCredentials({
    IdentityPoolId: 'us-east-1:7aadae5c-7106-4616-b5a1-1e24ed4a8b93',
});
const iotEndpoint = 'a1i7z9mmi9v21.iot.us-east-1.amazonaws.com';

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
  constructor () {
    super()
    // Should probably not use globals
  }

  _write (chunk, encoding, cb) {
    const message = chunk
    console.log('Jim sent', typeof message)
    client.publish(iotTopic, message)
  }

  _read (size) {
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

      const iotTopic = '/serverless/pubsub';

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
        client.on('message', (topic, message) => {
          console.log('Jim received', typeof message)
          stream.push(message)
        })
        pump(
          stream,
          dat.archive.replicate({live: true, encrypt: false}),
          stream
        )
      })

  });

}

run()


