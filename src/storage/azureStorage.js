const streamifier = require('streamifier');
const azure = require('azure-storage');

const azureStorage = (config) => {
  const blobService = azure.createBlobService(config.accountName, config.accountKey);

  const put = (filename, buffer) => new Promise((resolve, reject) => {
    const stream = streamifier.createReadStream(buffer);

    blobService.createBlockBlobFromStream(
      config.container,
      filename,
      stream,
      buffer.length,

      (err, response) => {
        if (err) reject(err);
        else resolve({
          name: filename,
          url: `https://${config.accountName}.blob.core.windows.net/${config.container}/${filename}`
        });
      }
    )

  });

  return {
    put
  };

};

module.exports = azureStorage;
