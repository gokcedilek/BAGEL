/**
 * @fileoverview gRPC-Web generated client stub for coord
 * @enhanceable
 * @public
 */

// GENERATED CODE -- DO NOT EDIT!


/* eslint-disable */
// @ts-nocheck



const grpc = {};
grpc.web = require('grpc-web');

const proto = {};
proto.coord = require('./coord_pb.js');

/**
 * @param {string} hostname
 * @param {?Object} credentials
 * @param {?grpc.web.ClientOptions} options
 * @constructor
 * @struct
 * @final
 */
proto.coord.CoordClient =
    function(hostname, credentials, options) {
  if (!options) options = {};
  options.format = 'text';

  /**
   * @private @const {!grpc.web.GrpcWebClientBase} The client
   */
  this.client_ = new grpc.web.GrpcWebClientBase(options);

  /**
   * @private @const {string} The hostname
   */
  this.hostname_ = hostname;

};


/**
 * @param {string} hostname
 * @param {?Object} credentials
 * @param {?grpc.web.ClientOptions} options
 * @constructor
 * @struct
 * @final
 */
proto.coord.CoordPromiseClient =
    function(hostname, credentials, options) {
  if (!options) options = {};
  options.format = 'text';

  /**
   * @private @const {!grpc.web.GrpcWebClientBase} The client
   */
  this.client_ = new grpc.web.GrpcWebClientBase(options);

  /**
   * @private @const {string} The hostname
   */
  this.hostname_ = hostname;

};


/**
 * @const
 * @type {!grpc.web.MethodDescriptor<
 *   !proto.coord.Query,
 *   !proto.coord.QueryResult>}
 */
const methodDescriptor_Coord_StartQuery = new grpc.web.MethodDescriptor(
  '/coord.Coord/StartQuery',
  grpc.web.MethodType.UNARY,
  proto.coord.Query,
  proto.coord.QueryResult,
  /**
   * @param {!proto.coord.Query} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.coord.QueryResult.deserializeBinary
);


/**
 * @param {!proto.coord.Query} request The
 *     request proto
 * @param {?Object<string, string>} metadata User defined
 *     call metadata
 * @param {function(?grpc.web.RpcError, ?proto.coord.QueryResult)}
 *     callback The callback function(error, response)
 * @return {!grpc.web.ClientReadableStream<!proto.coord.QueryResult>|undefined}
 *     The XHR Node Readable Stream
 */
proto.coord.CoordClient.prototype.startQuery =
    function(request, metadata, callback) {
  return this.client_.rpcCall(this.hostname_ +
      '/coord.Coord/StartQuery',
      request,
      metadata || {},
      methodDescriptor_Coord_StartQuery,
      callback);
};


/**
 * @param {!proto.coord.Query} request The
 *     request proto
 * @param {?Object<string, string>=} metadata User defined
 *     call metadata
 * @return {!Promise<!proto.coord.QueryResult>}
 *     Promise that resolves to the response
 */
proto.coord.CoordPromiseClient.prototype.startQuery =
    function(request, metadata) {
  return this.client_.unaryCall(this.hostname_ +
      '/coord.Coord/StartQuery',
      request,
      metadata || {},
      methodDescriptor_Coord_StartQuery);
};


/**
 * @const
 * @type {!grpc.web.MethodDescriptor<
 *   !proto.coord.QueryProgressRequest,
 *   !proto.coord.QueryProgressResponse>}
 */
const methodDescriptor_Coord_QueryProgress = new grpc.web.MethodDescriptor(
  '/coord.Coord/QueryProgress',
  grpc.web.MethodType.SERVER_STREAMING,
  proto.coord.QueryProgressRequest,
  proto.coord.QueryProgressResponse,
  /**
   * @param {!proto.coord.QueryProgressRequest} request
   * @return {!Uint8Array}
   */
  function(request) {
    return request.serializeBinary();
  },
  proto.coord.QueryProgressResponse.deserializeBinary
);


/**
 * @param {!proto.coord.QueryProgressRequest} request The request proto
 * @param {?Object<string, string>=} metadata User defined
 *     call metadata
 * @return {!grpc.web.ClientReadableStream<!proto.coord.QueryProgressResponse>}
 *     The XHR Node Readable Stream
 */
proto.coord.CoordClient.prototype.queryProgress =
    function(request, metadata) {
  return this.client_.serverStreaming(this.hostname_ +
      '/coord.Coord/QueryProgress',
      request,
      metadata || {},
      methodDescriptor_Coord_QueryProgress);
};


/**
 * @param {!proto.coord.QueryProgressRequest} request The request proto
 * @param {?Object<string, string>=} metadata User defined
 *     call metadata
 * @return {!grpc.web.ClientReadableStream<!proto.coord.QueryProgressResponse>}
 *     The XHR Node Readable Stream
 */
proto.coord.CoordPromiseClient.prototype.queryProgress =
    function(request, metadata) {
  return this.client_.serverStreaming(this.hostname_ +
      '/coord.Coord/QueryProgress',
      request,
      metadata || {},
      methodDescriptor_Coord_QueryProgress);
};


module.exports = proto.coord;

