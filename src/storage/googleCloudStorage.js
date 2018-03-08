/* Work in progress. Incomplete, untested. */
const googleCloudStorage = require('@google-cloud/storage')

const googleCloudStorageDriver = (config) => {
  const gcs = googleCloudStorage({
    projectId: config.projectId,
    keyFilename: config.keyFilename
  })

  const bucket = config.bucket
  ? Promise.resolve(gcs.bucket(config.bucket))
  : Promise.reject('No bucket specified')

  const put = (name, data) => bucket()
    .then((bucket) =>
      new Promise((resolve, reject) => {
        const stream = bucket.file(name).createWriteStream()
        stream.on('error', reject)
        stream.on('close', resolve)
        stream.write(data)
        stream.write
      })
    )

  return {put}
}
module.exports = googleCloudStorageDriver
