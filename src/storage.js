const googleCloudStorage = require('./storage/googleCloudStorage');
const azureStorage = require('./storage/azureStorage');

/**
 * @class Storage
 * @classdesc An abstract interface for dealing with any cloud storage provider.
 */

/**
 * @memberof Storage#
 * @function put
 * @param {string} name
 * @param {Buffer} data
 * @returns {File}
 */

/**
 * @class File
 * @classdesc A representation of a file stored in the cloud.
 * @property {string} name
 * @property {string} url - Publically accessible URL for the file
 * @property {string} contentType
 */

/**
  * Creates a storage provider for a given driver.
  * Example:
  * storage({
  *   driver: 'google',
  *   projectId: 'foo',
  *   keyFilename: 'key.json',
  *   bucket: 'bar'
  * })
  *
  * @param {object} config - Object with a 'driver' property and other properties
  *   needed by the driver.
  * @returns {Storage}
  */
const storage = (config) => new Promise((resolve, reject) =>
  !config ? reject('No storage config given.') :
  config.driver === 'google' ? resolve(googleCloudStorage(config)) :
  config.driver === 'minio' ? resolve(minioStorage(config)) :
  config.driver === 'azure' ? resolve(azureStorage(config)) :
  reject(`Unknown storage driver: ${config.driver}`)
);

module.exports = storage;
