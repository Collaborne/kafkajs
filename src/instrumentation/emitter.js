const EventEmitter = require('events')
const InstrumentationEvent = require('./event')
const { KafkaJSError } = require('../errors')

module.exports = class InstrumentationEventEmitter {
  constructor() {
    this.emitter = new EventEmitter()
    /** @type {InstrumentationEventEmitter[]} */
    this.forwarders = []
  }

  /**
   * @param {string} eventName
   * @param {Object} payload
   */
  emit(eventName, payload) {
    if (!eventName) {
      throw new KafkaJSError('Invalid event name', { retriable: false })
    }

    if (this.forwarders.length > 0 || this.emitter.listenerCount(eventName) > 0) {
      const event = new InstrumentationEvent(eventName, payload)
      this.emitter.emit(eventName, event)
      this.forwarders.forEach(forwarder => forwarder.emitForwarded(eventName, event))
    }
  }

  /**
   * @private
   */
  emitForwarded(eventName, event) {
    this.emitter.emit(eventName, event)
  }

  /**
   * @param {string} eventName
   * @param {Function} listener
   * @returns {Function} removeListener
   */
  addListener(eventName, listener) {
    this.emitter.addListener(eventName, listener)
    return () => this.emitter.removeListener(eventName, listener)
  }

  forward(anotherInstrumentationEmitter) {
    this.forwarders.push(anotherInstrumentationEmitter)
    return () => this.forwarders.remove(anotherInstrumentationEmitter)
  }
}
