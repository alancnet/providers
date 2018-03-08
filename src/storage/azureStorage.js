const streamifier = require('streamifier')
const azure = require('azure-storage')

const azureStorage = (config) => {
  const blobService = azure.createBlobService(config.accountName, config.accountKey)

  const put = (filename, buffer, retry) => new Promise((resolve, reject) => {
    const stream = streamifier.createReadStream(buffer)

    blobService.createBlockBlobFromStream(
      config.container,
      filename,
      stream,
      buffer.length,

      (err, response) => {
        if (err) {
          if (retry === 100) return reject(err)
          console.error(err, 'Retrying...')
          return put(filename, buffer, (retry || 0) + 1)
        } else {
          resolve({
            name: filename,
            url: `https://${config.accountName}.blob.core.windows.net/${config.container}/${filename}`
          })
        }
      }
    )
  })

  return {
    put
  }
}

module.exports = azureStorage
