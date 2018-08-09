var BigNumber = require('bignumber.js'),
    db = require('../db')

module.exports = function putRecord(store, data, cb) {

  var key = data.StreamName, metaDb = store.metaDb, streamDb = store.getStreamDb(data.DeliveryStreamName)

  metaDb.lock(key, function(release) {
    cb = release(cb)

    store.getStream(data.DeliveryStreamName, function(err, stream) {
      if (err) return cb(err)

      if (!~['ACTIVE', 'UPDATING'].indexOf(stream.StreamStatus)) {
        return cb(db.clientError('ResourceNotFoundException',
          'Stream ' + data.DeliveryStreamName + ' under account ' + metaDb.awsAccountId + ' not found.'))
      }

      var hashKey, shardIx, shardId, shardCreateTime

      hashKey = db.partitionKeyToHashKey('fake_partition_key')

      for (var i = 0; i < stream.Shards.length; i++) {
        if (stream.Shards[i].SequenceNumberRange.EndingSequenceNumber == null &&
            hashKey.comparedTo(stream.Shards[i].HashKeyRange.StartingHashKey) >= 0 &&
            hashKey.comparedTo(stream.Shards[i].HashKeyRange.EndingHashKey) <= 0) {
          shardIx = i
          shardId = stream.Shards[i].ShardId
          shardCreateTime = db.parseSequence(
            stream.Shards[i].SequenceNumberRange.StartingSequenceNumber).shardCreateTime
          break
        }
      }

      var seqIxIx = Math.floor(shardIx / 5), now = Math.max(Date.now(), shardCreateTime)

      // Ensure that the first record will always be above the stream start sequence
      if (!stream._seqIx[seqIxIx])
        stream._seqIx[seqIxIx] = shardCreateTime == now ? 1 : 0

      var seqNum = db.stringifySequence({
        shardCreateTime: shardCreateTime,
        shardIx: shardIx,
        seqIx: stream._seqIx[seqIxIx],
        seqTime: now,
      })

      var streamKey = db.shardIxToHex(shardIx) + '/' + seqNum

      stream._seqIx[seqIxIx]++

      metaDb.put(key, stream, function(err) {
        if (err) return cb(err)

        var record = {
          PartitionKey: data.PartitionKey,
          Data: data.Record.Data,
          ApproximateArrivalTimestamp: now / 1000,
        }

        streamDb.put(streamKey, record, function(err) {
          if (err) return cb(err)
          cb(null, {RecordId: seqNum})
        })
      })
    })
  })
}
