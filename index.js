const AWS = require("aws-sdk");
const Transform = require('stream').Transform;

class GlueError {
  constructor(message, code) {
    this.message = message;
    this.code = code
  }
  toString() {
    return `Error ${this.code}: ${this.message}`;
  }
}

class GlueClient {
  constructor(s3Prefix, glueJobName, awsRegion, bucket) {
    this.bucket = bucket;
    this.s3Prefix = s3Prefix;
    this.glueJobName = glueJobName;
    this.region = awsRegion;
    AWS.config.update({region:awsRegion});
    this.s3 = new AWS.S3();
    this.glue = new AWS.Glue();
  }

  getGlueJob(name) {
    const params = {
      JobName: name||this.glueJobName
    }

    return new Promise( (resolve, reject) => {
      this.glue.getJob(params, (err, data) => {
        if (err) {
          // console.log(err);
          if (err.code == 'EntityNotFoundException')
            return reject(new GlueError('Job not found', 404));
          else return reject(new GlueError('Unable to get job details.', 500));
        }
        this.glueScript = data.Job.Command.ScriptLocation.replace(`s3://${this.bucket}/`,'');
        return resolve(data.Job);
      })
    });
  }

  createGlueJob(description, name) {
    var params = {
      Command: { /* required */
        Name: 'glueetl',
        ScriptLocation: `s3://${this.bucket}/${this.s3Prefix}/${this.glueJobName}`
      },
      Name: name||this.glueJobName, /* required */
      Role: 'arn:aws:iam::117232195543:role/AWSGlueServiceRole', /* required */
      DefaultArguments: {
        '--TempDir': `s3://aws-glue-temporary-117232195543-us-east-1/${this.glueJobName}`,
        '--enable-metrics': '',
        '--job-bookmark-option': 'job-bookmark-disable',
        '--job-language': 'python'
      },
      Description: description,
      ExecutionProperty: {
        MaxConcurrentRuns: 1
      },
      MaxCapacity: 10.0,
      MaxRetries: 0,
      Timeout: 2880
    };
    return new Promise( (resolve, reject) => {
      this.glue.createJob(params, (err, data) => {
        if (err) {
          console.log(err);
          return reject(new GlueError('Unable to create job.', 500));
        }
        this.glueScript = params.Command.ScriptLocation.replace(`s3://${this.bucket}/`,'');
        return resolve(data.Name);
      })
    });
  }

  createGlueJobIfNotExists(name, description) {
    return new Promise( (resolve, reject) => {
      this.getGlueJob(name).then( (result) => {
        return resolve({status: 'Already exists', data: result});
      }, (err) => {
        if (err.code == 'EntityNotFoundException') {
          this.createGlueJob(description, name).then((result) => {
            return resolve({status: 'Job created', data: result})
          }, (err) => {
            return reject(new GlueError('Unable to create job'));
          })
        }
        else {
          return reject(new GlueError('Unable to get Job details.', 500));
        }
      })
    });
  }

  startDevEndpoint(name, role, securityGroups, subnet, keys, numNodes) {
    return new Promise( (resolve, reject) => {
      this.getDevEndpointStatus(name).then(
        (result) => {
          return resolve(result);
        },
        (err) => {
          if (err.code == 'EntityNotFoundException') {
            var params = {
                  EndpointName: name, /* required */
                  RoleArn: role, /* required */
                  NumberOfNodes: numNodes,
                  PublicKeys: keys,
                  SecurityGroupIds: securityGroups,
                  SubnetId: subnet
                };
            this.glue.createDevEndpoint(params, (err, data) => {
              if (err) {
                console.log(err);
                return reject(err)
              }
              console.log(data);
              return resolve(data.EndpointName);
            });
          }
          else {
            return reject(err);
          }
        }
      );

    });
  }

  getDevEndpointStatus(name) {
    return new Promise( (resolve, reject) => {
      var params = {
            EndpointName: name
          };
      this.glue.getDevEndpoint(params, (err, data) => {
        if (err) {
          console.log(err);
          return reject(err)
        }
        const statusData = {
          Status: data.DevEndpoint.Status,
          Name: data.DevEndpoint.Name,
          PrivateAddress: data.DevEndpoint.PrivateAddress
        }
        return resolve(statusData);
      });
    });
  }

  stopDevEndpoint(name) {
    return new Promise( (resolve, reject) => {
      var params = {
            EndpointName: name
          };
      this.glue.deleteDevEndpoint(params, (err, data) => {
        if (err) {
          console.log(err);
          return reject(err)
        }
        return resolve();
      });
    })
  }

  updateJobScript(key, script) {
    var content = script;
    if (Buffer.isBuffer(script) || (script.isBuffer && script.isBuffer()) ) {
      content = String(script.contents)
    }
    var base64Data = new Buffer(content, 'binary');
    var params = {
      Bucket: this.bucket,
      Key: key,
      Body: base64Data,
      ACL: 'public-read'
    }
    return new Promise( (resolve, reject) => {
      this.s3.putObject(params, (err, data) => {
        if (err) {
          console.log(err);
          return reject(new GlueError('Unable to update script.', 500));
        }
        resolve();
      });
    })
  }

  glueUpdateScript(options) {
    var transformStream = new Transform({objectMode: true});
    var me = this;
    const key = `${this.s3Prefix}/${this.glueJobName}`
    /**
   * @param {Buffer|string} file
   * @param {string=} encoding - ignored if file contains a Buffer
   * @param {function(Error, object)} callback - Call this function (optionally with an
   *          error argument and data) when you are done processing the supplied chunk.
   */
  transformStream._transform = (file, encoding, callback) => {
    this.getGlueJob().then((result) => {
      this.updateJobScript(key, file).then((result) => {
        return callback(null, file);
      }, (err) => {
        return callback(new GlueError('Unable to update job script'));
      })
    }, (err) => {
      if (err.code == 404) {
        if (options.createJob) {
          this.createJob('Job criado automaticamente').then((result) => {
            this.updateJobScript(key, file).then((result) => {
              return callback();
            }, (err) => {
              return callback(new GlueError('Unable to update job script'));
            })
          }, (err) => {
            return callback(new GlueError('Unable to create job'));
          })
        }
        else return callback(new GlueError('Job not found, enable creation', 404));
      }
      else {
        return callback(new GlueError('Unable to get Job details.', 500));
      }
    });
  };

  return transformStream;
  }
}

module.exports = GlueClient;
