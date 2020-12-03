const Broker = require('../broker')
const createRetry = require('../retry')
const shuffle = require('../utils/shuffle')
const arrayDiff = require('../utils/arrayDiff')
const { KafkaJSBrokerNotFound, KafkaJSProtocolError } = require('../errors')

const { keys, assign, values } = Object
const hasBrokerBeenReplaced = (broker, { host, port, rack }) =>
  broker.connection.host !== host ||
  broker.connection.port !== port ||
  broker.connection.rack !== rack

module.exports = class BrokerPool {
  /**
   * @param {object} options
   * @param {import("./connectionBuilder").ConnectionBuilder} options.connectionBuilder
   * @param {Logger} options.logger
   * @param {Object} options.retry
   * @param {number} options.authenticationTimeout
   * @param {number} options.reauthenticationThreshold
   * @param {number} options.metadataMaxAge
   */
  constructor({
    connectionBuilder,
    logger,
    retry,
    allowAutoTopicCreation,
    authenticationTimeout,
    reauthenticationThreshold,
    metadataMaxAge,
  }) {
    this.rootLogger = logger
    this.connectionBuilder = connectionBuilder
    this.metadataMaxAge = metadataMaxAge || 0
    this.logger = logger.namespace('BrokerPool')
    this.retrier = createRetry(assign({}, retry))

    this.createBroker = options =>
      new Broker({
        allowAutoTopicCreation,
        authenticationTimeout,
        reauthenticationThreshold,
        ...options,
      })

    this.brokers = {}
    /** @type {Broker | null} */
    this.seedBroker = null
    /** @type {import("../../types").BrokerMetadata | null} */
    this.metadata = null
    this.metadataExpireAt = null
    this.versions = null
    this.supportAuthenticationProtocol = null

    /** @type {{[topic: string]: Promise<null>}} */
    this.pendingMetadataTopicPromises = {}
    /** @type {{[topic: string]: () => void}} */
    this.pendingMetadataTopicResolves = {}
    /** @type {{[topic: string]: (err: Error) => void}} */
    this.pendingMetadataTopicRejects = {}
    this.refreshMetadataRunning = false
  }

  /**
   * @public
   * @returns {Boolean}
   */
  hasConnectedBrokers() {
    const brokers = values(this.brokers)
    return (
      !!brokers.find(broker => broker.isConnected()) ||
      (this.seedBroker ? this.seedBroker.isConnected() : false)
    )
  }

  async createSeedBroker() {
    if (this.seedBroker) {
      await this.seedBroker.disconnect()
    }

    this.seedBroker = this.createBroker({
      connection: await this.connectionBuilder.build(),
      logger: this.rootLogger,
    })
  }

  /**
   * @public
   * @returns {Promise<null>}
   */
  async connect() {
    if (this.hasConnectedBrokers()) {
      return
    }

    if (!this.seedBroker) {
      await this.createSeedBroker()
    }

    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        await this.seedBroker.connect()
        this.versions = this.seedBroker.versions
      } catch (e) {
        if (e.name === 'KafkaJSConnectionError' || e.type === 'ILLEGAL_SASL_STATE') {
          // Connection builder will always rotate the seed broker
          await this.createSeedBroker()
          this.logger.error(
            `Failed to connect to seed broker, trying another broker from the list: ${e.message}`,
            { retryCount, retryTime }
          )
        } else {
          this.logger.error(e.message, { retryCount, retryTime })
        }

        if (e.retriable) throw e
        bail(e)
      }
    })
  }

  /**
   * @public
   * @returns {Promise}
   */
  async disconnect() {
    this.seedBroker && (await this.seedBroker.disconnect())
    await Promise.all(values(this.brokers).map(broker => broker.disconnect()))

    this.brokers = {}
    this.metadata = null
    this.versions = null
    this.supportAuthenticationProtocol = null
  }

  /**
   * @public
   * @param {String} host
   * @param {Number} port
   */
  removeBroker({ host, port }) {
    const removedBroker = values(this.brokers).find(
      broker => broker.connection.host === host && broker.connection.port === port
    )

    if (removedBroker) {
      delete this.brokers[removedBroker.nodeId]
      this.metadataExpireAt = null

      if (this.seedBroker.nodeId === removedBroker.nodeId) {
        this.seedBroker = shuffle(values(this.brokers))[0]
      }
    }
  }

  /**
   * @public
   * @param {Array<String>} topics topics that minimally should be known in the metadata afterwards
   * @returns {Promise<null>}
   */
  async refreshMetadata(topics) {
    const promises = []
    for (const topic of topics) {
      let promise = this.pendingMetadataTopicPromises[topic]
      if (!promise) {
        promise = this.pendingMetadataTopicPromises[topic] = new Promise((resolve, reject) => {
          const cleanup = () => {
            delete this.pendingMetadataTopicPromises[topic]
            delete this.pendingMetadataTopicResolves[topic]
            delete this.pendingMetadataTopicRejects[topic]
          }

          this.pendingMetadataTopicResolves[topic] = () => {
            cleanup()
            resolve()
          }
          this.pendingMetadataTopicRejects[topic] = err => {
            cleanup()
            reject(err)
          }
        })
      }
      promises.push(promise)
    }

    if (!this.refreshMetadataRunning) {
      void this.refreshMetadataInternal()
    }

    return Promise.all(promises)
  }

  /** @private */
  get pendingMetadataTopics() {
    return keys(this.pendingMetadataTopicPromises)
  }

  /** @private */
  async refreshMetadataInternal() {
    const getTargetTopics = topicMetadata => {
      const topics = this.pendingMetadataTopics
      if (!topicMetadata) {
        return topics
      }
      const targetTopics = topicMetadata.map(({ topic }) => topic)
      return topics.reduce(
        (result, topic) => (result.includes(topic) ? result : [...result, topic]),
        targetTopics
      )
    }

    this.refreshMetadataRunning = true
    try {
      while (this.pendingMetadataTopics.length > 0) {
        const broker = await this.findConnectedBroker()
        const { host: seedHost, port: seedPort } = this.seedBroker.connection

        await this.retrier(async (bail, retryCount, retryTime) => {
          try {
            // Refresh the metadata for all topics: The pool could be shared between different clusters,
            // each with their own target topics
            // In theory we could also try to just fetch the data for the given topics, and then combine the
            // existing metadata.
            const topicMetadata = this.metadata ? this.metadata.topicMetadata : undefined
            this.metadata = await broker.metadata(getTargetTopics(topicMetadata))
            this.metadataExpireAt = Date.now() + this.metadataMaxAge

            const replacedBrokers = []

            this.brokers = await this.metadata.brokers.reduce(
              async (resultPromise, { nodeId, host, port, rack }) => {
                const result = await resultPromise

                if (result[nodeId]) {
                  if (!hasBrokerBeenReplaced(result[nodeId], { host, port, rack })) {
                    return result
                  }

                  replacedBrokers.push(result[nodeId])
                }

                if (host === seedHost && port === seedPort) {
                  this.seedBroker.nodeId = nodeId
                  this.seedBroker.connection.rack = rack
                  return assign(result, {
                    [nodeId]: this.seedBroker,
                  })
                }

                return assign(result, {
                  [nodeId]: this.createBroker({
                    logger: this.rootLogger,
                    versions: this.versions,
                    supportAuthenticationProtocol: this.supportAuthenticationProtocol,
                    connection: await this.connectionBuilder.build({ host, port, rack }),
                    nodeId,
                  }),
                })
              },
              this.brokers
            )

            const freshBrokerIds = this.metadata.brokers.map(({ nodeId }) => `${nodeId}`).sort()
            const currentBrokerIds = keys(this.brokers).sort()
            const unusedBrokerIds = arrayDiff(currentBrokerIds, freshBrokerIds)

            const brokerDisconnects = unusedBrokerIds.map(nodeId => {
              const broker = this.brokers[nodeId]
              return broker.disconnect().then(() => {
                delete this.brokers[nodeId]
              })
            })

            const replacedBrokersDisconnects = replacedBrokers.map(broker => broker.disconnect())

            // Resolve all pending topics that are now known
            this.metadata.topicMetadata
              .map(({ topic }) => topic)
              .forEach(topic => {
                const resolvePendingMetadataTopic = this.pendingMetadataTopicResolves[topic]
                if (resolvePendingMetadataTopic) {
                  resolvePendingMetadataTopic()
                }
              })
            await Promise.all([...brokerDisconnects, ...replacedBrokersDisconnects])
          } catch (e) {
            if (e.type === 'LEADER_NOT_AVAILABLE') {
              throw e
            }

            bail(e)
          }
        })
      }
    } catch (err) {
      // Reject all pending requests, as these have assumed we would refresh for them
      values(this.pendingMetadataTopicRejects).forEach(reject => {
        reject(err)
      })
    } finally {
      this.refreshMetadataRunning = false
    }
  }

  /**
   * Only refreshes metadata if the data is stale according to the `metadataMaxAge` param or does not contain information about the provided topics
   *
   * @public
   * @param {Array<String>} topics
   * @returns {Promise<null>}
   */
  async refreshMetadataIfNecessary(topics) {
    const shouldRefresh =
      this.metadata == null ||
      this.metadataExpireAt == null ||
      Date.now() > this.metadataExpireAt ||
      !topics.every(topic =>
        this.metadata.topicMetadata.some(topicMetadata => topicMetadata.topic === topic)
      )

    if (shouldRefresh) {
      return this.refreshMetadata(topics)
    }
  }

  /**
   * @public
   * @param {string} nodeId
   * @returns {Promise<Broker>}
   */
  async findBroker({ nodeId }) {
    const broker = this.brokers[nodeId]

    if (!broker) {
      throw new KafkaJSBrokerNotFound(`Broker ${nodeId} not found in the cached metadata`)
    }

    await this.connectBroker(broker)
    return broker
  }

  /**
   * @public
   * @param {Promise<{ nodeId<String>, broker<Broker> }>} callback
   * @returns {Promise<null>}
   */
  async withBroker(callback) {
    const brokers = shuffle(keys(this.brokers))
    if (brokers.length === 0) {
      throw new KafkaJSBrokerNotFound('No brokers in the broker pool')
    }

    for (const nodeId of brokers) {
      const broker = await this.findBroker({ nodeId })
      try {
        return await callback({ nodeId, broker })
      } catch (e) {}
    }

    return null
  }

  /**
   * @public
   * @returns {Promise<Broker>}
   */
  async findConnectedBroker() {
    const nodeIds = shuffle(keys(this.brokers))
    const connectedBrokerId = nodeIds.find(nodeId => this.brokers[nodeId].isConnected())

    if (connectedBrokerId) {
      return await this.findBroker({ nodeId: connectedBrokerId })
    }

    // Cycle through the nodes until one connects
    for (const nodeId of nodeIds) {
      try {
        return await this.findBroker({ nodeId })
      } catch (e) {}
    }

    // Failed to connect to all known brokers, metadata might be old
    await this.connect()
    return this.seedBroker
  }

  /**
   * @private
   * @param {Broker} broker
   * @returns {Promise<null>}
   */
  async connectBroker(broker) {
    if (broker.isConnected()) {
      return
    }

    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        await broker.connect()
      } catch (e) {
        if (e.name === 'KafkaJSConnectionError' || e.type === 'ILLEGAL_SASL_STATE') {
          await broker.disconnect()
        }

        // To avoid reconnecting to an unavailable host, we bail on connection errors
        // and refresh metadata on a higher level before reconnecting
        if (e.name === 'KafkaJSConnectionError') {
          return bail(e)
        }

        if (e.type === 'ILLEGAL_SASL_STATE') {
          // Rebuild the connection since it can't recover from illegal SASL state
          broker.connection = await this.connectionBuilder.build({
            host: broker.connection.host,
            port: broker.connection.port,
            rack: broker.connection.rack,
          })

          this.logger.error(`Failed to connect to broker, reconnecting`, { retryCount, retryTime })
          throw new KafkaJSProtocolError(e, { retriable: true })
        }

        if (e.retriable) throw e
        this.logger.error(e, { retryCount, retryTime, stack: e.stack })
        bail(e)
      }
    })
  }

  forwardInstrumentationEvents(anotherInstrumentationEmitter) {
    this.connectionBuilder.forwardInstrumentationEvents(anotherInstrumentationEmitter)
  }
}
