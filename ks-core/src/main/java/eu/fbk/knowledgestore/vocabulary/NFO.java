package eu.fbk.knowledgestore.vocabulary;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Constants for the NEPOMUK File Ontology.
 * 
 * @see <a href="http://www.semanticdesktop.org/ontologies/2007/03/22/nfo">vocabulary
 *      specification</a>
 */
public final class NFO {

    /** Recommended prefix for the vocabulary namespace: "nfo". */
    public static final String PREFIX = "nfo";

    /** Vocabulary namespace: "http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#". */
    public static final String NAMESPACE = "http://www.semanticdesktop.org"
            + "/ontologies/2007/03/22/nfo#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    // CLASSES

    /** Class nfo:Application. */
    public static final URI APPLICATION = createURI("Application");

    /** Class nfo:Archive. */
    public static final URI ARCHIVE = createURI("Archive");

    /** Class nfo:ArchiveItem. */
    public static final URI ARCHIVE_ITEM = createURI("ArchiveItem");

    /** Class nfo:Attachment. */
    public static final URI ATTACHMENT = createURI("Attachment");

    /** Class nfo:Audio. */
    public static final URI AUDIO = createURI("Audio");

    /** Class nfo:Bookmark. */
    public static final URI BOOKMARK = createURI("Bookmark");

    /** Class nfo:BookmarkFolder. */
    public static final URI BOOKMARK_FOLDER = createURI("BookmarkFolder");

    /** Class nfo:CompressionType. */
    public static final URI COMPRESSION_TYPE = createURI("CompressionType");

    /** Class nfo:Cursor. */
    public static final URI CURSOR = createURI("Cursor");

    /** Class nfo:DataContainer. */
    public static final URI DATA_CONTAINER = createURI("DataContainer");

    /** Class nfo:DeletedResource. */
    public static final URI DELETED_RESOURCE = createURI("DeletedResource");

    /** Class nfo:Document. */
    public static final URI DOCUMENT = createURI("Document");

    /** Class nfo:EmbeddedFileDataObject. */
    public static final URI EMBEDDED_FILE_DATA_OBJECT = createURI("EmbeddedFileDataObject");

    /** Class nfo:Executable. */
    public static final URI EXECUTABLE = createURI("Executable");

    /** Class nfo:FileDataObject. */
    public static final URI FILE_DATA_OBJECT = createURI("FileDataObject");

    /** Class nfo:FileHash. */
    public static final URI FILE_HASH = createURI("FileHash");

    /** Class nfo:Filesystem. */
    public static final URI FILESYSTEM = createURI("Filesystem");

    /** Class nfo:FilesystemImage. */
    public static final URI FILESYSTEM_IMAGE = createURI("FilesystemImage");

    /** Class nfo:Folder. */
    public static final URI FOLDER = createURI("Folder");

    /** Class nfo:Font. */
    public static final URI FONT = createURI("Font");

    /** Class nfo:HardDiskPartition. */
    public static final URI HARD_DISK_PARTITION = createURI("HardDiskPartition");

    /** Class nfo:HtmlDocument. */
    public static final URI HTML_DOCUMENT = createURI("HtmlDocument");

    /** Class nfo:Icon. */
    public static final URI ICON = createURI("Icon");

    /** Class nfo:Image. */
    public static final URI IMAGE = createURI("Image");

    /** Class nfo:Media. */
    public static final URI MEDIA = createURI("Media");

    /** Class nfo:MediaFileListEntry. */
    public static final URI MEDIA_FILE_LIST_ENTRY = createURI("MediaFileListEntry");

    /** Class nfo:MediaList. */
    public static final URI MEDIA_LIST = createURI("MediaList");

    /** Class nfo:MediaStream. */
    public static final URI MEDIA_STREAM = createURI("MediaStream");

    /** Class nfo:MindMap. */
    public static final URI MIND_MAP = createURI("MindMap");

    /** Class nfo:OperatingSystem. */
    public static final URI OPERATING_SYSTEM = createURI("OperatingSystem");

    /** Class nfo:PaginatedTextDocument. */
    public static final URI PAGINATED_TEXT_DOCUMENT = createURI("PaginatedTextDocument");

    /** Class nfo:PlainTextDocument. */
    public static final URI PLAIN_TEXT_DOCUMENT = createURI("PlainTextDocument");

    /** Class nfo:Presentation. */
    public static final URI PRESENTATION = createURI("Presentation");

    /** Class nfo:RasterImage. */
    public static final URI RASTER_IMAGE = createURI("RasterImage");

    /** Class nfo:RemoteDataObject. */
    public static final URI REMOTE_DATA_OBJECT = createURI("RemoteDataObject");

    /** Class nfo:RemotePortAddress. */
    public static final URI REMOTE_PORT_ADDRESS = createURI("RemotePortAddress");

    /** Class nfo:Software. */
    public static final URI SOFTWARE = createURI("Software");

    /** Class nfo:SoftwareItem. */
    public static final URI SOFTWARE_ITEM = createURI("SoftwareItem");

    /** Class nfo:SoftwareService. */
    public static final URI SOFTWARE_SERVICE = createURI("SoftwareService");

    /** Class nfo:SourceCode. */
    public static final URI SOURCE_CODE = createURI("SourceCode");

    /** Class nfo:Spreadsheet. */
    public static final URI SPREADSHEET = createURI("Spreadsheet");

    /** Class nfo:TextDocument. */
    public static final URI TEXT_DOCUMENT = createURI("TextDocument");

    /** Class nfo:Trash. */
    public static final URI TRASH = createURI("Trash");

    /** Class nfo:VectorImage. */
    public static final URI VECTOR_IMAGE = createURI("VectorImage");

    /** Class nfo:Video. */
    public static final URI VIDEO = createURI("Video");

    /** Class nfo:Visual. */
    public static final URI VISUAL = createURI("Visual");

    /** Class nfo:Website. */
    public static final URI WEBSITE = createURI("Website");

    // PROPERTIES

    /** Property nfo:aspectRatio. */
    public static final URI ASPECT_RATIO = createURI("aspectRatio");

    /** Property nfo:averageBitrate. */
    public static final URI AVERAGE_BITRATE = createURI("averageBitrate");

    /** Property nfo:belongsToContainer. */
    public static final URI BELONGS_TO_CONTAINER = createURI("belongsToContainer");

    /** Property nfo:bitDepth. */
    public static final URI BIT_DEPTH = createURI("bitDepth");

    /** Property nfo:bitrateType. */
    public static final URI BITRATE_TYPE = createURI("bitrateType");

    /** Property nfo:bitsPerSample. */
    public static final URI BITS_PER_SAMPLE = createURI("bitsPerSample");

    /** Property nfo:bookmarks. */
    public static final URI BOOKMARKS = createURI("bookmarks");

    /** Property nfo:channels. */
    public static final URI CHANNELS = createURI("channels");

    /** Property nfo:characterCount. */
    public static final URI CHARACTER_COUNT = createURI("characterCount");

    /** Property nfo:codec. */
    public static final URI CODEC = createURI("codec");

    /** Property nfo:colorDepth. */
    public static final URI COLOR_DEPTH = createURI("colorDepth");

    /** Property nfo:commentCharacterCount. */
    public static final URI COMMENT_CHARACTER_COUNT = createURI("commentCharacterCount");

    /** Property nfo:compressionType. */
    public static final URI COMPRESSION_TYPE_PROPERTY = createURI("compressionType");

    /** Property nfo:conflicts. */
    public static final URI CONFLICTS = createURI("conflicts");

    /** Property nfo:containsBookmark. */
    public static final URI CONTAINS_BOOKMARK = createURI("containsBookmark");

    /** Property nfo:containsBookmarkFolder. */
    public static final URI CONTAINS_BOOKMARK_FOLDER = createURI("containsBookmarkFolder");

    /** Property nfo:count. */
    public static final URI COUNT = createURI("count");

    /** Property nfo:definesClass. */
    public static final URI DEFINES_CLASS = createURI("definesClass");

    /** Property nfo:definesFunction. */
    public static final URI DEFINES_FUNCTION = createURI("definesFunction");

    /** Property nfo:definesGlobalVariable. */
    public static final URI DEFINES_GLOBAL_VARIABLE = createURI("definesGlobalVariable");

    /** Property nfo:deletionDate. */
    public static final URI DELETION_DATE = createURI("deletionDate");

    /** Property nfo:duration. */
    public static final URI DURATION = createURI("duration");

    /** Property nfo:encoding. */
    public static final URI ENCODING = createURI("encoding");

    /** Property nfo:fileCreated. */
    public static final URI FILE_CREATED = createURI("fileCreated");

    /** Property nfo:fileLastAccessed. */
    public static final URI FILE_LAST_ACCESSED = createURI("fileLastAccessed");

    /** Property nfo:fileLastModified. */
    public static final URI FILE_LAST_MODIFIED = createURI("fileLastModified");

    /** Property nfo:fileName. */
    public static final URI FILE_NAME = createURI("fileName");

    /** Property nfo:fileOwner. */
    public static final URI FILE_OWNER = createURI("fileOwner");

    /** Property nfo:fileSize. */
    public static final URI FILE_SIZE = createURI("fileSize");

    /** Property nfo:fileUrl. */
    public static final URI FILE_URL = createURI("fileUrl");

    /** Property nfo:fontFamily. */
    public static final URI FONT_FAMILY = createURI("fontFamily");

    /** Property nfo:foundry. */
    public static final URI FOUNDRY = createURI("foundry");

    /** Property nfo:frameCount. */
    public static final URI FRAME_COUNT = createURI("frameCount");

    /** Property nfo:frameRate. */
    public static final URI FRAME_RATE = createURI("frameRate");

    /** Property nfo:frontChannels. */
    public static final URI FRONT_CHANNELS = createURI("frontChannels");

    /** Property nfo:hasHash. */
    public static final URI HAS_HASH = createURI("hasHash");

    /** Property nfo:hasMediaFileListEntry. */
    public static final URI HAS_MEDIA_FILE_LIST_ENTRY = createURI("hasMediaFileListEntry");

    /** Property nfo:hasMediaStream. */
    public static final URI HAS_MEDIA_STREAM = createURI("hasMediaStream");

    /** Property nfo:hashAlgorithm. */
    public static final URI HASH_ALGORITHM = createURI("hashAlgorithm");

    /** Property nfo:hashValue. */
    public static final URI HASH_VALUE = createURI("hashValue");

    /** Property nfo:height. */
    public static final URI HEIGHT = createURI("height");

    /** Property nfo:horizontalResolution. */
    public static final URI HORIZONTAL_RESOLUTION = createURI("horizontalResolution");

    /** Property nfo:interlaceMode. */
    public static final URI INTERLACE_MODE = createURI("interlaceMode");

    /** Property nfo:isPasswordProtected. */
    public static final URI IS_PASSWORD_PROTECTED = createURI("isPasswordProtected");

    /** Property nfo:lfeChannels. */
    public static final URI LFE_CHANNELS = createURI("lfeChannels");

    /** Property nfo:lineCount. */
    public static final URI LINE_COUNT = createURI("lineCount");

    /** Property nfo:originalLocation. */
    public static final URI ORIGINAL_LOCATION = createURI("originalLocation");

    /** Property nfo:pageCount. */
    public static final URI PAGE_COUNT = createURI("pageCount");

    /** Property nfo:permissions. */
    public static final URI PERMISSIONS = createURI("permissions");

    /** Property nfo:programmingLanguage. */
    public static final URI PROGRAMMING_LANGUAGE = createURI("programmingLanguage");

    /** Property nfo:rate. */
    public static final URI RATE = createURI("rate");

    /** Property nfo:rearChannels. */
    public static final URI REAR_CHANNELS = createURI("rearChannels");

    /** Property nfo:sampleCount. */
    public static final URI SAMPLE_COUNT = createURI("sampleCount");

    /** Property nfo:sampleRate. */
    public static final URI SAMPLE_RATE = createURI("sampleRate");

    /** Property nfo:sideChannels. */
    public static final URI SIDE_CHANNELS = createURI("sideChannels");

    /** Property nfo:supercedes. */
    public static final URI SUPERCEDES = createURI("supercedes");

    /** Property nfo:uncompressedSize. */
    public static final URI UNCOMPRESSED_SIZE = createURI("uncompressedSize");

    /** Property nfo:verticalResolution. */
    public static final URI VERTICAL_RESOLUTION = createURI("verticalResolution");

    /** Property nfo:width. */
    public static final URI WIDTH = createURI("width");

    /** Property nfo:wordCount. */
    public static final URI WORD_COUNT = createURI("wordCount");

    // INDIVIDUALS

    /** Individual nfo:losslessCompressionType. */
    public static final URI LOSSLESS_COMPRESSION_TYPE = createURI("losslessCompressionType");

    /** Individual nfo:lossyCompressionType. */
    public static final URI LOSSY_COMPRESSION_TYPE = createURI("lossyCompressionType");

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private NFO() {
    }

}
