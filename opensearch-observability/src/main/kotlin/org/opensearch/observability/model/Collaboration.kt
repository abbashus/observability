package org.opensearch.observability.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.observability.ObservabilityPlugin
import org.opensearch.observability.util.fieldIfNotNull
import org.opensearch.observability.util.logger

/**
lastUpdatedTimeMs: <epoch_millis>
createdTimeMs: <epoch_millis>
tenant: ""
access: [
"User:admin",
"Role:own_index",
"Role:all_access",
"BERole:admin"
]
collaboration: #the document ID of this object becomes collaboration ID
type: TEXT | VIZ
text:
- pageId # notebookId in case of notebooks
- paragraphId # notebook paragraph Id
- lineId # unique identifier for a line
visualization:
- savedVisulizationId
- startTime : <epoch_millis>
- endTime : <epoch_millis>
tags: ["prod" , "dashboard" ]? [same as in Grafana, may be we can do search comments based on tags in a dedicated chat page]
resolved: true | false (defaults to false)
 */

internal data class Collaboration(
    val type: CollaborationDataType?,
//    val textInfo: TextInfo? // TODO: better name, can this be null
    val pageId: String?,
    val paragraphId: String?,
    val lineId: String?,
    val tags: String?,
    val resolved: Boolean
) : BaseObjectData {

    internal companion object {
        private val log by logger(Collaboration::class.java)
        private const val TYPE_TAG = "type"
//        private const val TEXT_INFO_TAG = "text"
        private const val PAGE_TAG = "pageId"
        private const val PARAGRAPH_TAG = "paragraphId"
        private const val LINE_TAG = "lineId"
        private const val TAGS_TAG = "tags"
        private const val RESOLVED_TAG = "resolved"

        /**
         * reader to create instance of class from writable.
         */
        val reader = Writeable.Reader { Collaboration(it) }

        /**
         * Parser to parse xContent
         */
        val xParser = XParser { parse(it) }

//        /**
//         * Parse the tag list from parser
//         * @param parser data referenced at parser
//         * @return created list of tags
//         */
//        private fun parseTagList(parser: XContentParser): List<String> {
//            val retList: MutableList<String> = mutableListOf()
//            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser)
//            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
//                retList.add(parser.text())
//            }
//            return retList
//        }

        /**
         * Parse the data from parser and create Notebook object
         * @param parser data referenced at parser
         * @return created Notebook object
         */
        fun parse(parser: XContentParser): Collaboration {
            var type: CollaborationDataType? = null
//            var textInfo: TextInfo? = null // TODO: better name, can this be null
            var pageId: String? = null
            var paragraphId: String? = null
            var lineId: String? = null
            var tags: String? = null
            var resolved: Boolean = false // TODO: check if false is right value here
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser)
            while (XContentParser.Token.END_OBJECT != parser.nextToken()) {
                val fieldName = parser.currentName()
                parser.nextToken()
                when (fieldName) {
                    TYPE_TAG -> type = CollaborationDataType.TEXT // TODO: make it generic
                    // TEXT_INFO_TAG -> textInfo = parser.text() // TODO: fix , add parser to TextInfo
                    PAGE_TAG -> pageId = parser.text()
                    PARAGRAPH_TAG -> paragraphId = parser.text()
                    LINE_TAG -> lineId = parser.text()
                    // TAGS_TAG -> tags = parseTagList(parser)
                    TAGS_TAG -> tags = parser.text()
                    RESOLVED_TAG -> resolved = parser.booleanValue()
                    else -> {
                        parser.skipChildren()
                        log.info("${ObservabilityPlugin.LOG_PREFIX}:Collaboration Skipping Unknown field $fieldName")
                    }
                }
            }
            log.info("====== Collaboration : start==========")
            log.info("$TYPE_TAG: $type")
            log.info("$PAGE_TAG: $pageId")
            log.info("$PARAGRAPH_TAG: $paragraphId")
            log.info("$LINE_TAG: $lineId")
            log.info("$TAGS_TAG: $tags")
            log.info("$RESOLVED_TAG: $resolved")
            log.info("====== Collaboration : end ==========")
            return Collaboration(type, pageId, paragraphId, lineId, tags, resolved)
        }
    }

    /**
     * create XContentBuilder from this object using [XContentFactory.jsonBuilder()]
     * @param params XContent parameters
     * @return created XContentBuilder object
     */
    fun toXContent(params: ToXContent.Params = ToXContent.EMPTY_PARAMS): XContentBuilder? {
        return toXContent(XContentFactory.jsonBuilder(), params)
    }

    /**
     * Constructor used in transport action communication.
     * @param input StreamInput stream to deserialize data from.
     */
    constructor(input: StreamInput) : this(
        type = input.readEnum(CollaborationDataType::class.java),
        pageId = input.readString(),
        paragraphId = input.readString(),
        lineId = input.readString(),
//        textInfo = input.readOptionalWriteable(TextInfo.reader),
        tags = input.readString(), // TODO change to list
        resolved = input.readBoolean(),
    )

    /**
     * {@inheritDoc}
     */
    override fun writeTo(output: StreamOutput) {
        output.writeEnum(type)
        output.writeString(pageId)
        output.writeString(paragraphId)
        output.writeString(lineId)
        output.writeString(tags)
//        output.writeOptionalWriteable(textInfo)
//        output.writeList(tags)
        output.writeBoolean(resolved)
    }

    /**
     * {@inheritDoc}
     */
    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
//        val xContentParams = params ?: RestTag.REST_OUTPUT_PARAMS
        builder!!
        builder.startObject()
            .fieldIfNotNull(TYPE_TAG, type)
            .fieldIfNotNull(PAGE_TAG, pageId)
            .fieldIfNotNull(PARAGRAPH_TAG, paragraphId)
            .fieldIfNotNull(LINE_TAG, lineId)
            .fieldIfNotNull(TAGS_TAG, tags)
            .fieldIfNotNull(RESOLVED_TAG, resolved)
//        if (paragraphs != null) {
//            builder.startArray(PARAGRAPHS_TAG)
//            paragraphs.forEach { it.toXContent(builder, xContentParams) }
//            builder.endArray()
//        }
        return builder.endObject()
    }

//    /**
//     * TextInfo source data class //TODO : better name for TextInfo
//     */
//    internal data class TextInfo(
//        val pageId: String?,
//        val paragraphId: String?,
//        val lineId: String?
//    ): BaseModel {
//        internal companion object {
//            private const val OUTPUT_TAG = "output"
//            private const val INPUT_TAG = "input"
//            private const val DATE_CREATED_TAG = "dateCreated"
//            private const val DATE_MODIFIED_TAG = "dateModified"
//            private const val ID_TAG = "id"
//
//            /**
//             * reader to create instance of class from writable.
//             */
//            val reader = Writeable.Reader { TextInfo(it) }
//
//            /**
//             * Parser to parse xContent
//             */
//            val xParser = XParser { parse(it) }
//
//            /**
//             * Parse the item list from parser
//             * @param parser data referenced at parser
//             * @return created list of items
//             */
//            private fun parseItemList(parser: XContentParser): List<Notebook.Output> {
//                val retList: MutableList<Notebook.Output> = mutableListOf()
//                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser)
//                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
//                    retList.add(Notebook.Output.parse(parser))
//                }
//                return retList
//            }
//
//            /**
//             * Parse the data from parser and create Source object
//             * @param parser data referenced at parser
//             * @return created Source object
//             */
//            fun parse(parser: XContentParser): Notebook.Paragraph {
//                var output: List<Notebook.Output>? = null
//                var input: Notebook.Input? = null
//                var dateCreated: String? = null
//                var dateModified: String? = null
//                var id: String? = null
//                XContentParserUtils.ensureExpectedToken(
//                    XContentParser.Token.START_OBJECT,
//                    parser.currentToken(),
//                    parser
//                )
//                while (XContentParser.Token.END_OBJECT != parser.nextToken()) {
//                    val fieldName = parser.currentName()
//                    parser.nextToken()
//                    when (fieldName) {
//                        OUTPUT_TAG -> output = parseItemList(parser)
//                        INPUT_TAG -> input = Notebook.Input.parse(parser)
//                        DATE_CREATED_TAG -> dateCreated = parser.text()
//                        DATE_MODIFIED_TAG -> dateModified = parser.text()
//                        ID_TAG -> id = parser.text()
//                        else -> {
//                            parser.skipChildren()
//                            Notebook.log.info("${ObservabilityPlugin.LOG_PREFIX}:Source Skipping Unknown field $fieldName")
//                        }
//                    }
//                }
//                output ?: throw IllegalArgumentException("$OUTPUT_TAG field absent")
//                input ?: throw IllegalArgumentException("$INPUT_TAG field absent")
//                dateCreated ?: throw IllegalArgumentException("$DATE_CREATED_TAG field absent")
//                dateModified ?: throw IllegalArgumentException("$DATE_MODIFIED_TAG field absent")
//                id ?: throw IllegalArgumentException("$ID_TAG field absent")
//                return Notebook.Paragraph(output, input, dateCreated, dateModified, id)
//            }
//        }
//
//        constructor(streamInput: StreamInput) : this(
//            output = streamInput.readList(Notebook.Output.reader),
//            input = streamInput.readOptionalWriteable(Notebook.Input.reader),
//            dateCreated = streamInput.readString(),
//            dateModified = streamInput.readString(),
//            id = streamInput.readString()
//        )
//
//        override fun writeTo(streamOutput: StreamOutput) {
//            streamOutput.writeCollection(output)
//            streamOutput.writeOptionalWriteable(input)
//            streamOutput.writeString(dateCreated)
//            streamOutput.writeString(dateModified)
//            streamOutput.writeString(id)
//        }
//
//        /**
//         * {@inheritDoc}
//         */
//        override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
//            val xContentParams = params ?: RestTag.REST_OUTPUT_PARAMS
//            builder!!
//            builder.startObject()
//                .startArray(OUTPUT_TAG)
//            output.forEach { it.toXContent(builder, xContentParams) }
//            builder.endArray()
//                .field(INPUT_TAG, input)
//                .field(DATE_CREATED_TAG, dateCreated)
//                .field(DATE_MODIFIED_TAG, dateModified)
//                .field(ID_TAG, id)
//            return builder.endObject()
//        }
//    }
//
}
