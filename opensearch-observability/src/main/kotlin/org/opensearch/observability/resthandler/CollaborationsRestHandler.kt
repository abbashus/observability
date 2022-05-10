/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.observability.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.commons.utils.logger
import org.opensearch.observability.ObservabilityPlugin.Companion.BASE_COLLABORATION_URI
import org.opensearch.observability.action.CreateCollaborationObjectAction
import org.opensearch.observability.action.CreateCollaborationObjectRequest
import org.opensearch.observability.model.RestTag.COLLABORATION_ID_FIELD
// import org.opensearch.observability.model.RestTag.COLLABORATION_ID_LIST_FIELD
import org.opensearch.observability.model.RestTag.COMMENT_ID_FIELD
import org.opensearch.observability.util.contentParserNextToken
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.DELETE
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.RestRequest.Method.PUT
import org.opensearch.rest.RestStatus

/**
 * Rest handler for observability object lifecycle management.
 * This handler uses [CollaborationActions].
 */
internal class CollaborationsRestHandler : BaseRestHandler() {
    companion object {
        private const val COLLABORATION_ACTION = "collaboration_actions"
        private const val COLLABORATION_URL = "$BASE_COLLABORATION_URI/collaborations"
        private const val COMMENT_URL = "$COLLABORATION_URL/{$COLLABORATION_ID_FIELD}/comment"
        private val log by logger(CollaborationsRestHandler::class.java)
    }

    /**
     * {@inheritDoc}
     */
    override fun getName(): String {
        return COLLABORATION_ACTION
    }

    /**
     * {@inheritDoc}
     */
    override fun routes(): List<Route> {
        return listOf(
            /**
             * Creates a new collaboration
             * Request URL: POST COLLABORATION_URL
             * Request body: Ref [org.opensearch.observability.model.CreateObservabilityObjectRequest]
             * Response body: Ref [org.opensearch.observability.model.CreateObservabilityObjectResponse]
             */
            Route(POST, COLLABORATION_URL),
            /**
             * Update collaboration object
             * Request URL: PUT COLLABORATION_URL/{collaborationId}
             * Request body: Ref [org.opensearch.observability.model.UpdateObservabilityObjectRequest]
             * Response body: Ref [org.opensearch.observability.model.UpdateObservabilityObjectResponse]
             */
            Route(PUT, "$COLLABORATION_URL/{$COLLABORATION_ID_FIELD}"),
            /**
             * Get a collaboration object
             * Request URL: GET COLLABORATION_URL/{collaborationId}
             * Request body: Ref [org.opensearch.observability.model.GetObservabilityObjectRequest]
             * Response body: Ref [org.opensearch.observability.model.GetObservabilityObjectResponse]
             */
            Route(GET, "$COLLABORATION_URL/{$COLLABORATION_ID_FIELD}"),
            Route(GET, COLLABORATION_URL),
            /**
             * Delete a collaboration object
             * Request URL: DELETE COLLABORATION_URL/{collaborationId}
             * Request body: Ref [org.opensearch.observability.model.DeleteObservabilityObjectRequest]
             * Response body: Ref [org.opensearch.observability.model.DeleteObservabilityObjectResponse]
             */
            Route(DELETE, "$COLLABORATION_URL/{$COLLABORATION_ID_FIELD}"),
//            Route(DELETE, "$COLLABORATION_URL"), // TODO: do we need this?
            /**
             * Creates a new comment
             * Request URL: POST COMMENT_URL
             * Request body: Ref [org.opensearch.observability.model.CreateObservabilityObjectRequest]
             * Response body: Ref [org.opensearch.observability.model.CreateObservabilityObjectResponse]
             */
            Route(POST, COMMENT_URL),
            /**
             * Update comment object
             * Request URL: PUT COMMENT_URL/{commentId}
             * Request body: Ref [org.opensearch.observability.model.UpdateObservabilityObjectRequest]
             * Response body: Ref [org.opensearch.observability.model.UpdateObservabilityObjectResponse]
             */
            Route(PUT, "$COMMENT_URL/{$COMMENT_ID_FIELD}"),
            /**
             * Get a comment object
             * Request URL: GET COMMENT_URL/{commentId}
             * Request body: Ref [org.opensearch.observability.model.GetObservabilityObjectRequest]
             * Response body: Ref [org.opensearch.observability.model.GetObservabilityObjectResponse]
             */
            Route(GET, "$COMMENT_URL/{$COMMENT_ID_FIELD}"),
            Route(GET, COMMENT_URL),
            /**
             * Delete a comment object
             * Request URL: DELETE COMMENT_URL/{commentId}
             * Request body: Ref [org.opensearch.observability.model.DeleteObservabilityObjectRequest]
             * Response body: Ref [org.opensearch.observability.model.DeleteObservabilityObjectResponse]
             */
            Route(DELETE, "$COMMENT_URL/{$COMMENT_ID_FIELD}"),
//            Route(DELETE, "$COMMENT_URL"), //TODO: do we need this?

        )
    }

    /**
     * {@inheritDoc}
     */
    override fun responseParams(): Set<String> {
        return setOf(
            COMMENT_ID_FIELD,
            COLLABORATION_ID_FIELD
        )
    }

    private fun executePostRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        return RestChannelConsumer {
            client.execute(
                CreateCollaborationObjectAction.ACTION_TYPE,
                CreateCollaborationObjectRequest.parse(request.contentParserNextToken()),
                RestResponseToXContentListener(it)
            )
        }
    }

//    private fun executePutRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
//        return RestChannelConsumer {
//            client.execute(
//                UpdateObservabilityObjectAction.ACTION_TYPE,
//                UpdateObservabilityObjectRequest.parse(request.contentParserNextToken(), request.param(OBJECT_ID_FIELD)),
//                RestResponseToXContentListener(it)
//            )
//        }
//    }

//    private fun executeGetRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
//        val objectId: String? = request.param(OBJECT_ID_FIELD)
//        val objectIdListString: String? = request.param(OBJECT_ID_LIST_FIELD)
//        val objectIdList = getObjectIdSet(objectId, objectIdListString)
//        val types: EnumSet<ObservabilityObjectType> = getTypesSet(request.param(OBJECT_TYPE_FIELD))
//        val sortField: String? = request.param(SORT_FIELD_FIELD)
//        val sortOrderString: String? = request.param(SORT_ORDER_FIELD)
//        val sortOrder: SortOrder? = if (sortOrderString == null) {
//            null
//        } else {
//            SortOrder.fromString(sortOrderString)
//        }
//        val fromIndex = request.param(FROM_INDEX_FIELD)?.toIntOrNull() ?: 0
//        val maxItems = request.param(MAX_ITEMS_FIELD)?.toIntOrNull() ?: PluginSettings.defaultItemsQueryCount
//        val filterParams = request.params()
//            .filter { ObservabilityQueryHelper.FILTER_PARAMS.contains(it.key) }
//            .map { Pair(it.key, request.param(it.key)) }
//            .toMap()
//        log.info(
//            "$LOG_PREFIX:executeGetRequest idList:$objectIdList types:$types, from:$fromIndex, maxItems:$maxItems," +
//                " sortField:$sortField, sortOrder=$sortOrder, filters=$filterParams"
//        )
//        return RestChannelConsumer {
//            client.execute(
//                GetObservabilityObjectAction.ACTION_TYPE,
//                GetObservabilityObjectRequest(
//                    objectIdList,
//                    types,
//                    fromIndex,
//                    maxItems,
//                    sortField,
//                    sortOrder,
//                    filterParams
//                ),
//                RestResponseToXContentListener(it)
//            )
//        }
//    }

//    private fun executeDeleteRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
//        val collaborationId: String? = request.param(COLLABORATION_ID_FIELD)
//        val collaborationIdSet: Set<String> =
//            request.paramAsStringArray(COLLABORATION_ID_FIELD, arrayOf(collaborationId))
//                .filter { s -> !s.isNullOrBlank() }
//                .toSet()
//        return RestChannelConsumer {
//            if (collaborationIdSet.isEmpty()) {
//                it.sendResponse(
//                    BytesRestResponse(
//                        RestStatus.BAD_REQUEST,
//                        "Either $COLLABORATION_ID_FIELD or $COLLABORATION_ID_LIST_FIELD is required"
//                    )
//                )
//            } else {
//                client.execute(
//                    gs
//                        DeleteObservabilityObjectAction.ACTION_TYPE,
//                    DeleteObservabilityObjectRequest(objectIdSet),
//                    RestResponseToXContentListener(it)
//                )
//            }
//        }
//    }

    /**
     * {@inheritDoc}
     */
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.info(" uri(): ${request.uri()}")
        log.info(" path(): ${request.path()}")
        log.info(" rawPath: ${request.rawPath()}")
        return when (request.method()) {
            POST -> executePostRequest(request, client)
//            PUT -> executePutRequest(request, client)
//            GET -> executeGetRequest(request, client)
//            DELETE -> executeDeleteRequest(request, client)
            else -> RestChannelConsumer {
                it.sendResponse(BytesRestResponse(RestStatus.METHOD_NOT_ALLOWED, "${request.method()} is not allowed"))
            }
        }
    }

//    private fun getObjectIdSet(objectId: String?, objectIdList: String?): Set<String> {
//        var retIds: Set<String> = setOf()
//        if (objectId != null) {
//            retIds = setOf(objectId)
//        }
//        if (objectIdList != null) {
//            retIds = objectIdList.split(",").union(retIds)
//        }
//        return retIds
//    }
//
//    private fun getTypesSet(typesString: String?): EnumSet<ObservabilityObjectType> {
//        var types: EnumSet<ObservabilityObjectType> = EnumSet.noneOf(ObservabilityObjectType::class.java)
//        typesString?.split(",")?.forEach { types.add(ObservabilityObjectType.fromTagOrDefault(it)) }
//        return types
//    }
}
