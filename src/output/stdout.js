const _ = require('lodash');
const stdout = (_config) => {
  const config = _.defaults(_config, {
    continueOnError: false
  });
  return {
    next: console.log,
    error: (err) => {
      console.error(err);
      if (!config.continueOnError) process.exit(1);
    }
  };
};

module.exports = stdout;
