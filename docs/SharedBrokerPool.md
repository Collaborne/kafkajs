---
id: shared-broker-pool
title: Sharing Broker Pools
---

The default behavior of KafkaJS is to treat each consumer, producer, and admin client independently from each other. In particular this means network connections to the configured brokers will be created again for every one of these. For larger applications this can lead to high resource usage both on the client side as well as on the broker side, and it may lead to throttling.

It is possible to influence this behavior for consumers and producers by manually configuring "broker pools", and sharing these.

## <a name="consumer"></a> Example

```javascript
const brokerPool = kafka.brokerPool({ })
await brokerPool.connect()
```

This broker pool can be provided to the `kafka.consumer` and `kafka.producer` methods, instead of the options that would typically configure the pool.

```javascript
const consumer = kafka.consumer({ groupId: 'my-group', brokerPool })
```

```javascript
const producer = kafka.producer({ brokerPool })
```

The consumer and producer can then be used as described in [Consuming Messages](Consuming.md) and [Producing Messages](Producing.md), with the caveat that the `connect` and `disconnect` methods will affect the complete pool, and therefore should typicaly not be used.

## <a name="options"></a> Options

```javascript
kafka.brokerPool({
  metadataMaxAge: <Number>,
  allowAutoTopicCreation: <Boolean>,
  maxInFlightRequests: <Number>,
})
```

| option                 | description                                                                                                                                                                                                                                                                                                                                        | default                           |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| metadataMaxAge         | The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions                                                                                                                                                       | `300000` (5 minutes)              |
| allowAutoTopicCreation | Allow topic creation when querying metadata for non-existent topics                                                                                                                                                                                                                                                                                | `true`                            |
| maxInFlightRequests | Max number of requests that may be in progress at any time. If falsey then no limit.                                    | `null` _(no limit)_ |
