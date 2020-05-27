/**
 * Generator that processes the given promises, and yields their result in the order of them resolving.
 */
async function* BufferedAsyncIterator(promises) {
  /** Queue of promises in order of resolution */
  const promisesQueue = []
  /** Queue of {resolve, reject} in the same order as `promisesQueue` */
  const resolveRejectQueue = []

  promises.forEach(promise => {
    // Create a new promise into the promises queue, and keep the {resolve,reject}
    // in the resolveRejectQueue
    let resolvePromise
    let rejectPromise
    promisesQueue.push(
      new Promise((resolve, reject) => {
        resolvePromise = resolve
        rejectPromise = reject
      })
    )
    resolveRejectQueue.push({ resolve: resolvePromise, reject: rejectPromise })

    // When the promise resolves pick the next available {resolve, reject}, and
    // through that resolve the next promise in the queue
    promise.then(
      result => {
        const { resolve } = resolveRejectQueue.pop()
        resolve(result)
      },
      err => {
        const { reject } = resolveRejectQueue.pop()
        reject(err)
      }
    )
  })

  // While there are promises left pick the next one, wait for it, and yield the result
  while (promisesQueue.length > 0) {
    const nextPromise = promisesQueue.pop()
    const result = await nextPromise
    yield result
  }
}

module.exports = BufferedAsyncIterator
