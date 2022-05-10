package org.opensearch.observability.action

import org.opensearch.OpenSearchStatusException
import org.opensearch.commons.authuser.User
import org.opensearch.observability.ObservabilityPlugin
import org.opensearch.observability.index.CollaborationIndex
import org.opensearch.observability.model.CollaborationObjectDoc
import org.opensearch.observability.security.UserAccessManager
import org.opensearch.observability.util.logger
import org.opensearch.rest.RestStatus
import java.time.Instant

/**
 * CollaborationObject index operation actions.
 */
internal object CollaborationActions {

    private val log by logger(CollaborationActions::class.java)

    /**
     * Create new CollaborationObject
     * @param request [CreateCollaborationObjectRequest] object
     * @return [CreateCollaborationObjectResponse]
     */
    fun create(request: CreateCollaborationObjectRequest, user: User?): CreateCollaborationObjectResponse {
        log.info("${ObservabilityPlugin.LOG_PREFIX}:CollaborationObject-create") // TODO: remove Observability word
        UserAccessManager.validateUser(user)
        val currentTime = Instant.now()
        val objectDoc = CollaborationObjectDoc(
            "ignore",
            currentTime,
            currentTime,
            UserAccessManager.getUserTenant(user),
            UserAccessManager.getAllAccessInfo(user),
            request.type,
            request.objectData
        )
        val docId = CollaborationIndex.createCollaborationObject(objectDoc, request.collaborationId)
        docId ?: throw OpenSearchStatusException(
            "CollaborationObject Creation failed",
            RestStatus.INTERNAL_SERVER_ERROR
        )
        return CreateCollaborationObjectResponse(docId)
    }

//    /**
//     * Delete ObservabilityObject
//     * @param request [DeleteObservabilityObjectRequest] object
//     * @param user the user info object
//     * @return [DeleteObservabilityObjectResponse]
//     */
//    fun delete(request: DeleteObservabilityObjectRequest, user: User?): DeleteObservabilityObjectResponse {
//        ObservabilityActions.log.info("${ObservabilityPlugin.LOG_PREFIX}:ObservabilityObject-delete ${request.objectIds}")
//        return if (request.objectIds.size == 1) {
//            ObservabilityActions.delete(request.objectIds.first(), user)
//        } else {
//            ObservabilityActions.delete(request.objectIds, user)
//        }
//    }
}
