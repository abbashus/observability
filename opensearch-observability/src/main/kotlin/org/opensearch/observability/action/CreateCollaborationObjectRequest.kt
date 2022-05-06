package org.opensearch.observability.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.commons.utils.fieldIfNotNull
import org.opensearch.commons.utils.logger
import org.opensearch.observability.model.BaseObjectData
import org.opensearch.observability.model.CollaborationObjectType
import org.opensearch.observability.model.CollaborationObjectDataProperties
import org.opensearch.observability.model.RestTag.COLLABORATION_ID_FIELD
import java.io.IOException

/**
 * Action request for creating new configuration.
 */
internal class CreateCollaborationObjectRequest : ActionRequest, ToXContentObject {
    val collaborationId: String?
    val type: CollaborationObjectType
    val objectData: BaseObjectData?

    companion object {
        private val log by logger(CreateCollaborationObjectRequest::class.java)

        /**
         * reader to create instance of class from writable.
         */
        val reader = Writeable.Reader { CreateCollaborationObjectRequest(it) }

        /**
         * Creator used in REST communication.
         * @param parser XContentParser to deserialize data from.
         * @param id optional id to use if missed in XContent
         */
        @JvmStatic
        @Throws(IOException::class)
        fun parse(parser: XContentParser, id: String? = null): CreateCollaborationObjectRequest {
            var collaborationId: String? = id
            var type: CollaborationObjectType? = null
            var baseObjectData: BaseObjectData? = null

            XContentParserUtils.ensureExpectedToken(
                XContentParser.Token.START_OBJECT,
                parser.currentToken(),
                parser
            )
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = parser.currentName()
                parser.nextToken()
                when (fieldName) {
                    COLLABORATION_ID_FIELD -> collaborationId = parser.text()
                    else -> {
                        val objectTypeForTag = CollaborationObjectType.fromTagOrDefault(fieldName)
                        if (objectTypeForTag != CollaborationObjectType.NONE && baseObjectData == null) {
                            baseObjectData = CollaborationObjectDataProperties.createObjectData(objectTypeForTag, parser)
                            type = objectTypeForTag
                        } else {
                            parser.skipChildren()
                            log.info("Unexpected field: $fieldName, while parsing CreateCollaborationObjectRequest")
                        }
                    }
                }
            }
            type ?: throw IllegalArgumentException("Object type field absent")
            baseObjectData ?: throw IllegalArgumentException("Object data field absent")
            return CreateCollaborationObjectRequest(collaborationId, type, baseObjectData)
        }
    }

    /**
     * {@inheritDoc}
     */
    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        builder!!
        return builder.startObject()
            .fieldIfNotNull(COLLABORATION_ID_FIELD, collaborationId)
            .field(type.tag, objectData)
            .endObject()
    }

    /**
     * constructor for creating the class
     * @param collaborationId optional id to use for CollaborationObject
     * @param type type of CollaborationObject
     * @param objectData the CollaborationObject
     */
    constructor(collaborationId: String? = null, type: CollaborationObjectType, objectData: BaseObjectData) {
        this.collaborationId = collaborationId
        this.type = type
        this.objectData = objectData
    }

    /**
     * {@inheritDoc}
     */
    @Throws(IOException::class)
    constructor(input: StreamInput) : super(input) {
        collaborationId = input.readOptionalString()
        type = input.readEnum(CollaborationObjectType::class.java)
        objectData = input.readOptionalWriteable(CollaborationObjectDataProperties.getReaderForObjectType(input.readEnum(CollaborationObjectType::class.java)))
    }

    /**
     * {@inheritDoc}
     */
    @Throws(IOException::class)
    override fun writeTo(output: StreamOutput) {
        super.writeTo(output)
        output.writeOptionalString(collaborationId)
        output.writeEnum(type)
        output.writeEnum(type)
        output.writeOptionalWriteable(objectData)
    }

    /**
     * {@inheritDoc}
     */
    override fun validate(): ActionRequestValidationException? {
        return null
    }
}