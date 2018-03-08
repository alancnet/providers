const fs = require('fs');
const _ = require('lodash');
const mkdirp = require('mkdirp');
const filenamify = require('filenamify');

const fileStorage = (_config) => {
  const config = _.defaults(_config, {
    directory: './files'
  });

  const getPath = (name) => `${config.directory}/${filenamify(name)}`;

  return new Promise((resolve, reject) => {
    mkdirp(config.directory, (err) => {
      if (err) return reject(err);
      resolve({
          get: (name) => new Promise((resolve, reject) => {
            const path = getPath(name);
            fs.exists(path, (exists) => {
              if (exists) {
                fs.readFile(path, (err, data) => {
                  if (err) reject(err);
                  else resolve(data);
                })
              } else {
                reject(new Error('File does not exist'));
              }
            })
          }),

          put: (name, data) => new Promise((resolve, reject) =>
            fs.writeFile(getPath(name), data, (err) => err ? reject(err) : resolve({
              name: name
            }))
          )
        })
    })
  })
}

module.exports = fileStorage;
