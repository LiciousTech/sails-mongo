//  ███████╗███████╗██╗     ███████╗ ██████╗████████╗     █████╗  ██████╗████████╗██╗ ██████╗ ███╗   ██╗
//  ██╔════╝██╔════╝██║     ██╔════╝██╔════╝╚══██╔══╝    ██╔══██╗██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║
//  ███████╗█████╗  ██║     █████╗  ██║        ██║       ███████║██║        ██║   ██║██║   ██║██╔██╗ ██║
//  ╚════██║██╔══╝  ██║     ██╔══╝  ██║        ██║       ██╔══██║██║        ██║   ██║██║   ██║██║╚██╗██║
//  ███████║███████╗███████╗███████╗╚██████╗   ██║       ██║  ██║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║
//  ╚══════╝╚══════╝╚══════╝╚══════╝ ╚═════╝   ╚═╝       ╚═╝  ╚═╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
//

module.exports = require('machine').build({


  friendlyName: 'Select',


  description: 'Find record(s) in the database.',


  inputs: {

    datastore: {
      description: 'The datastore to use for connections.',
      extendedDescription: 'Datastores represent the config and manager required to obtain an active database connection.',
      required: true,
      readOnly: true,
      example: '==='
    },

    models: {
      description: 'An object containing all of the model definitions that have been registered.',
      required: true,
      example: '==='
    },

    query: {
      description: 'A valid stage three Waterline query.',
      required: true,
      example: '==='
    }

  },


  exits: {

    success: {
      description: 'The results of the select query.',
      outputFriendlyName: 'Report',
      outputDescription: 'The `records` key is an array of physical records.  The `meta` key is reserved for passing back adapter-specific information to userland and/or Waterline.',
      outputExample: '===' //{ records: [ {===} ], meta: '===' }
    },

  },


  fn: function select(inputs, exits) {
    // Dependencies
    var _ = require('@sailshq/lodash');
    var Helpers = require('./private');


    // Store the Query input for easier access
    var query = inputs.query;


    // Find the model definition
    var model = inputs.models[query.using];
    if (!model) {
      return exits.error(new Error('No `'+query.using+'` model has been registered with this adapter.  Were any unexpected modifications made to the stage 3 query?  Could the adapter\'s internal state have been corrupted?  (This error is usually due to a bug in this adapter\'s implementation.)'));
    }//-•


    // Get mongo collection (and spawn a new connection)
    var mongoCollection = inputs.datastore.manager.collection(query.using);


    // Normalize the WHERE criteria into a mongo style where clause
    var where;
    try {
      where = Helpers.query.normalizeWhere(query.criteria.where);
    } catch (e) {
      return exits.error(e);
    }

    // Transform the stage-3 query sort array into a Mongo sort dictionary.
    var sort = _.map(query.criteria.sort, function mapSort(sortObj) {
      var key = _.first(_.keys(sortObj));
      var sortCriteria = [];
      var sortDirection = sortObj[key].toLowerCase() === 'asc' ? 1 : -1;
      sortCriteria.push(key);
      sortCriteria.push(sortDirection);
      return sortCriteria;
    });

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // TODO: deal with case where `select` is not defined (i.e. when a model is `schema: false`)
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    // Transform the stage-3 query select array into a Mongo projection dictionary.
    var projection = _.reduce(query.criteria.select, function reduceProjection(memo, colName) {
      memo[colName] = 1;
      return memo;
    }, {});

    // Create the initial Mongo query.
    var mongoDeferred;
    try {
      mongoDeferred = mongoCollection.find(where).project(projection).sort(sort);
    } catch (err) { return exits.error(err); }

    // Add in limit if necessary.
    if (query.criteria.limit) {
      mongoDeferred.limit(query.criteria.limit);
    }

    // Add in skip if necessary.
    if (query.criteria.skip) {
      mongoDeferred.skip(query.criteria.skip);
    }

    // Find the documents in the db.
    mongoDeferred.toArray(function findCb(err, records) {
      if (err) {
        return exits.error(err);
      }

      // Process each record to normalize output
      try {
        Helpers.query.processEachRecord({
          records: records,
          identity: model.identity,
          orm: { collections: inputs.models }
        });
      } catch (e) { return exits.error(e); }

      return exits.success({ records: records });

    }); // </ mongoDeferred.toArray() >
  }
});
