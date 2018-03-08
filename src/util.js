var exitHooked = false

const hookExit = () => {
  if (!exitHooked) {
    process.on('SIGINT', () => process.exit(0))
    exitHooked = true
  }
}

const onExit = (callback) => {
  hookExit()
  const oldExit = process.exit
  var sideEffect = (code) => {
    try {
      const newCode = callback(code)
      if (newCode && newCode.then && newCode.catch) {
        // Promise
        newCode.then((newCode) => {
          oldExit(newCode === undefined ? code : newCode)
        }).catch((err) => {
          console.error(err)
          oldExit(code || 1)
        })
      } else {
        // Syncronous
        oldExit(newCode === undefined ? code : newCode)
      }
    } catch (err) {
      console.error(err)
      oldExit(code || 1)
    }
  }
  process.exit = (code) => sideEffect(code)
  return () => {
    // Releases exit side effect
    sideEffect = () => {}
  }
}

module.exports = {
  onExit
}
