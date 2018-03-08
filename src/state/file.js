const fs = require('fs');
const _ = require('lodash');
const mkdirp = require('mkdirp');
const filenamify = require('filenamify');
const writeFile = require('crash-safe-write-file').writeFile;


const fileState = (_config) => {
  const config = _.defaults(_config, {
    name: undefined,
    directory: './state'
  });

  return new Promise((resolve, reject) => {
    mkdirp(config.directory, (err) => {
      if (err) return reject(err);
      const filename = `${config.directory}/${filenamify(config.name)}.state.json`;

      resolve({
        get: () => new Promise((resolve, reject) => {
          fs.readFile(filename, (err, data) => {
            if (err) reject(err);
            else resolve(JSON.parse(data.toString()));
          })
        }),
        put: (state) => new Promise((resolve, reject) => {
          writeFile(filename, JSON.stringify(state), (err) => {
            if (err) reject(err);
            else resolve();
          })
        })
      });

    })
  })
}

module.exports = fileState;
