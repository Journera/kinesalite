var BigNumber = require('bignumber.js'),
    db = require('../db')

module.exports = function putRecordBatch(store, data, cb) {

  var key = data.DeliveryStreamName, metaDb = store.metaDb, streamDb = store.getStreamDb(data.DeliveryStreamName)

  metaDb.lock(key, function(release) {
    cb = release(cb)

    store.getStream(data.DeliveryStreamName, function(err, stream) {
      if (err) return cb(err)

      if (!~['ACTIVE', 'UPDATING'].indexOf(stream.StreamStatus)) {
        return cb(db.clientError('ResourceNotFoundException',
          'Stream ' + data.DeliveryStreamName + ' under account ' + metaDb.awsAccountId + ' not found.'))
      }

      var batchOps = new Array(data.Records.length), returnRecords = new Array(data.Records.length),
        seqPieces = new Array(data.Records.length), record, hashKey, seqPiece, i

      for (i = 0; i < data.Records.length; i++) {
        record = data.Records[i]
        hashKey = db.partitionKeyToHashKey("fixed-partition-key")

        for (var j = 0; j < stream.Shards.length; j++) {
          if (stream.Shards[j].SequenceNumberRange.EndingSequenceNumber == null &&
              hashKey.comparedTo(stream.Shards[j].HashKeyRange.StartingHashKey) >= 0 &&
              hashKey.comparedTo(stream.Shards[j].HashKeyRange.EndingHashKey) <= 0) {
            seqPieces[i] = {
              shardIx: j,
              shardId: stream.Shards[j].ShardId,
              shardCreateTime: db.parseSequence(
                stream.Shards[j].SequenceNumberRange.StartingSequenceNumber).shardCreateTime,
            }
            break
          }
        }
      }

      // This appears to be the order that shards are processed in a PutRecords call
      // XXX: No longer true – shards can be processed simultaneously and do not appear to be deterministic
      var shardOrder = stream.Shards.length < 18 ?
        [15, 16, 14, 13, 10, 12, 11, 7, 5, 9, 8, 6, 4, 3, 2, 1, 0] : stream.Shards.length < 27 ?
          [25, 21, 23, 22, 24, 20, 15, 19, 16, 17, 18, 11, 14, 13, 10, 12, 9, 6, 7, 5, 8, 3, 0, 4, 2, 1] :
          [46, 45, 49, 47, 48, 40, 42, 41, 43, 44, 35, 38, 39, 37, 36, 31, 34, 33, 32, 30, 28, 26, 27, 29, 25, 22, 24, 20, 23, 21, 15, 16, 17, 19, 18, 11, 13, 12, 14, 10, 9, 7, 8, 6, 5, 1, 3, 0, 4, 2]

      // Unsure of order after shard 49, just process sequentially
      for (i = 50; i < stream.Shards.length; i++) {
        shardOrder.push(i)
      }

      shardOrder.forEach(function(shardIx) {
        if (shardIx >= stream.Shards.length) return

        for (i = 0; i < data.Records.length; i++) {
          record = data.Records[i]
          seqPiece = seqPieces[i]
          console.log('Publish %s', record.Data)

          if (seqPiece.shardIx != shardIx) continue

          var seqIxIx = Math.floor(shardIx / 5), now = Math.max(Date.now(), seqPiece.shardCreateTime)

          // Ensure that the first record will always be above the stream start sequence
          if (!stream._seqIx[seqIxIx])
            stream._seqIx[seqIxIx] = seqPiece.shardCreateTime == now ? 1 : 0

          var seqNum = db.stringifySequence({
            shardCreateTime: seqPiece.shardCreateTime,
            shardIx: shardIx,
            seqIx: stream._seqIx[seqIxIx],
            seqTime: now,
          })

          var streamKey = db.shardIxToHex(shardIx) + '/' + seqNum

          stream._seqIx[seqIxIx]++

          batchOps[i] = {
            type: 'put',
            key: streamKey,
            value: {
              PartitionKey: "fixed-partition-key",
              Data: record.Data,
              ApproximateArrivalTimestamp: now / 1000,
            },
          }

          returnRecords[i] = {RecordId: seqNum}
        }
      })

      metaDb.put(key, stream, function(err) {
        if (err) return cb(err)

        streamDb.batch(batchOps, {}, function(err) {
          if (err) return cb(err)
          cb(null, {FailedPutCount: 0, RequestResponses: returnRecords})
        })
      })
    })
  })
}
