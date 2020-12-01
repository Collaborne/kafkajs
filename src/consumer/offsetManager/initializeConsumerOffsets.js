const isInvalidOffset = require('./isInvalidOffset')
const { entries, assign } = Object

const indexPartitions = (obj, { partition, offset }) => assign(obj, { [partition]: offset })
const indexTopics = (obj, { topic, partitions }) =>
  assign(obj, { [topic]: partitions.reduce(indexPartitions, {}) })

module.exports = (consumerOffsets, topicOffsets) => {
  const indexedConsumerOffsets = consumerOffsets.reduce(indexTopics, {})
  const indexedTopicOffsets = topicOffsets.reduce(indexTopics, {})

  return entries(indexedConsumerOffsets).map(([topic, partitions]) => {
    return {
      topic,
      partitions: entries(partitions).map(([partition, offset]) => {
        const resolvedOffset = isInvalidOffset(offset)
          ? indexedTopicOffsets[topic][partition]
          : offset

        return { partition: Number(partition), offset: resolvedOffset }
      }),
    }
  })
}
