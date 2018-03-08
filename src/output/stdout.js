const stdout = (config) => ({
  next: console.log,
  error: console.error
});

module.exports = stdout;
