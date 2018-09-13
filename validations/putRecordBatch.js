exports.types = {
  DeliveryStreamName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  },
  Records: {
    type: 'List',
    notNull: true,
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 500,
    children: {
      type: 'Structure',
      children: {
        Data: {
          type: 'Blob',
          notNull: true,
          lengthLessThanOrEqual: 1048576,
        },
      },
    },
  },
}
