package io.floci.az.services.blob;

import io.floci.az.core.AzureErrorResponse;
import io.floci.az.core.AzureRequest;
import io.floci.az.core.AzureServiceHandler;
import io.floci.az.core.StoredObject;
import io.floci.az.core.XmlBuilder;
import io.floci.az.core.XmlUtils;
import io.floci.az.core.storage.StorageBackend;
import io.floci.az.core.storage.StorageFactory;
import io.floci.az.config.EmulatorConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@ApplicationScoped
public class BlobServiceHandler implements AzureServiceHandler {

    private static final Logger LOGGER = Logger.getLogger(BlobServiceHandler.class);
    private static final DateTimeFormatter RFC1123_DATE_TIME = DateTimeFormatter
            .ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US)
            .withZone(ZoneId.of("GMT"));

    private static final String NS_PREFIX  = "__ns__:";
    private static final String BLK_PREFIX = "__blk__:";
    private static final String USER_METADATA_PREFIX = "UserMeta:";
    private static final StoredObject NS_SENTINEL =
            new StoredObject("", new byte[0], Map.of(), Instant.EPOCH, "");

    /**
     * Matches {@code <Latest>}, {@code <Committed>}, or {@code <Uncommitted>} elements
     * inside a PutBlockList XML body — e.g. {@code <Latest>BASE64ID</Latest>}.
     */
    private static final Pattern BLOCK_LIST_PATTERN =
            Pattern.compile("<(?:Latest|Committed|Uncommitted)>([^<]+)</(?:Latest|Committed|Uncommitted)>");

    private final StorageBackend<String, StoredObject> store;
    private final Set<String> hierarchicalNamespaceAccounts;

    @Inject
    public BlobServiceHandler(StorageFactory storageFactory, EmulatorConfig config) {
        this.store = storageFactory.create("blob");
        this.hierarchicalNamespaceAccounts = Set.copyOf(config.services().blob().hierarchicalNamespaceAccounts().orElse(List.of()));
    }

    @Override
    public String getServiceType() {
        return "blob";
    }

    @Override
    public boolean canHandle(AzureRequest request) {
        return "blob".equals(request.serviceType());
    }

    @Override
    public Response handle(AzureRequest request) {
        String path = request.resourcePath();
        String method = request.method();
        Map<String, String> query = request.queryParams();

        LOGGER.infof("BlobService handling: %s %s", method, path);

        Response response;
        if (request.authContext() != null && !request.authContext().isValid()) {
            response = new AzureErrorResponse("AuthenticationFailed",
                    "Server failed to authenticate the request. Make sure the value of Authorization header "
                            + "is formed correctly including the signature.")
                    .toXmlResponse(Response.Status.FORBIDDEN.getStatusCode());
        } else if (path.isEmpty() || path.equals("/")) {
            if ("GET".equalsIgnoreCase(method) && "list".equals(query.get("comp"))) {
                response = listContainers(request);
            } else if ("service".equals(query.get("restype")) && "properties".equals(query.get("comp"))) {
                if ("GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method)) {
                    response = getBlobServiceProperties();
                } else {
                    response = Response.ok().build();
                }
            } else {
                response = new AzureErrorResponse("NotImplemented", "The requested operation is not implemented.")
                        .toXmlResponse(501);
            }
        } else {
            String[] parts = path.split("/", 2);
            String containerName = parts[0];
            String blobName = parts.length > 1 ? parts[1] : "";
            if (!blobName.isEmpty()) {
                Optional<String> normalizedBlobName = normalizePath(blobName);
                if (normalizedBlobName.isEmpty()) {
                    return new AzureErrorResponse("InvalidQueryParameterValue",
                            "Value for one of the query parameters specified in the request URI is invalid.")
                            .toXmlResponse(400);
                }
                blobName = normalizedBlobName.orElseThrow();
            }

            if (blobName.isEmpty()) {
                if ("GET".equalsIgnoreCase(method) && "filesystem".equals(query.get("resource"))) {
                    response = listDfsPaths(request, containerName);
                } else if ("GET".equalsIgnoreCase(method) && "list".equals(query.get("comp"))) {
                    response = listBlobs(request, containerName);
                } else if ("PUT".equalsIgnoreCase(method) && "container".equals(query.get("restype"))) {
                    response = createContainer(request, containerName);
                } else if ("DELETE".equalsIgnoreCase(method) && "container".equals(query.get("restype"))) {
                    response = deleteContainer(request, containerName);
                } else if (("GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method)) && "container".equals(query.get("restype"))) {
                    response = getContainer(request, containerName, "HEAD".equalsIgnoreCase(method));
                } else if ("GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method)) {
                    if (hierarchicalNamespaceAccounts.contains(request.accountName())) {
                        response = Response.ok("HEAD".equalsIgnoreCase(method) ? null : new byte[0])
                                .header(HttpHeaders.CONTENT_LENGTH, 0)
                                .build();
                    } else {
                        response = getBlob(request, containerName, "/", "HEAD".equalsIgnoreCase(method));
                    }
                } else {
                    response = new AzureErrorResponse("NotImplemented", "The requested operation is not implemented.")
                            .toXmlResponse(501);
                }
            } else {
                String comp = query.get("comp");
                if ("PUT".equalsIgnoreCase(method) && "metadata".equals(comp)) {
                    response = setBlobMetadata(request, containerName, blobName);
                } else if (("GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method))
                        && "metadata".equals(comp)) {
                    response = getBlobMetadata(request, containerName, blobName);
                } else if ("PUT".equalsIgnoreCase(method) && "block".equals(comp)) {
                    response = putBlock(request, containerName, blobName);
                } else if ("PUT".equalsIgnoreCase(method) && "blocklist".equals(comp)) {
                    response = putBlockList(request, containerName, blobName);
                } else if (("GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method))
                        && "blocklist".equals(comp)) {
                    response = getBlockList(request, containerName, blobName);
                } else if ("PUT".equalsIgnoreCase(method) && request.headers().getHeaderString("x-ms-rename-source") != null) {
                    response = renameDfsPath(request, containerName, blobName);
                } else if ("PUT".equalsIgnoreCase(method) && isDfsPathCreate(request)) {
                    response = createDfsPath(request, containerName, blobName, isDfsDirectoryCreate(request));
                } else if ("GET".equalsIgnoreCase(method) && "filesystem".equals(query.get("resource"))) {
                    response = listDfsPaths(request, containerName, blobName);
                } else if ("PUT".equalsIgnoreCase(method)) {
                    response = putBlob(request, containerName, blobName);
                } else if ("GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method)) {
                    response = getBlob(request, containerName, blobName, "HEAD".equalsIgnoreCase(method));
                } else if ("DELETE".equalsIgnoreCase(method)) {
                    response = deleteBlob(request, containerName, blobName);
                } else {
                    response = new AzureErrorResponse("NotImplemented", "The requested operation is not implemented.")
                            .toXmlResponse(501);
                }
            }
        }

        return Response.fromResponse(response)
                .header("x-ms-request-id", UUID.randomUUID().toString())
                .header("x-ms-version", request.headers().getHeaderString("x-ms-version"))
                .header("Date", RFC1123_DATE_TIME.format(Instant.now()))
                .build();
    }

    private Response getBlobServiceProperties() {
        String xml = new XmlBuilder()
            .start("StorageServiceProperties")
                .start("Logging")
                    .elem("Version", "1.0")
                    .elem("Delete", "false")
                    .elem("Read", "false")
                    .elem("Write", "false")
                    .start("RetentionPolicy").elem("Enabled", "false").end("RetentionPolicy")
                .end("Logging")
                .start("HourMetrics")
                    .elem("Version", "1.0")
                    .elem("Enabled", "false")
                    .start("RetentionPolicy").elem("Enabled", "false").end("RetentionPolicy")
                .end("HourMetrics")
                .start("MinuteMetrics")
                    .elem("Version", "1.0")
                    .elem("Enabled", "false")
                    .start("RetentionPolicy").elem("Enabled", "false").end("RetentionPolicy")
                .end("MinuteMetrics")
                .start("StaticWebsite").elem("Enabled", "false").end("StaticWebsite")
            .end("StorageServiceProperties")
            .toString();
        return Response.ok(xml, "application/xml").build();
    }

    private Response getContainer(AzureRequest request, String containerName, boolean headOnly) {
        if (store.get(nsKey(request.accountName(), containerName)).isEmpty()) {
            return new AzureErrorResponse("ContainerNotFound", "The specified container does not exist.")
                    .toXmlResponse(Response.Status.NOT_FOUND.getStatusCode());
        }
        return Response.ok()
                .header("Last-Modified", RFC1123_DATE_TIME.format(Instant.now()))
                .header("ETag", UUID.randomUUID().toString())
                .header("x-ms-has-immutability-policy", "false")
                .header("x-ms-has-legal-hold", "false")
                .build();
    }

    private Response createContainer(AzureRequest request, String containerName) {
        String key = nsKey(request.accountName(), containerName);
        if (store.get(key).isPresent()) {
            return new AzureErrorResponse("ContainerAlreadyExists", "The specified container already exists.")
                    .toXmlResponse(Response.Status.CONFLICT.getStatusCode());
        }
        store.put(key, NS_SENTINEL);
        return Response.status(Response.Status.CREATED)
                .header("Last-Modified", RFC1123_DATE_TIME.format(Instant.now()))
                .header("ETag", UUID.randomUUID().toString())
                .build();
    }

    private Response deleteContainer(AzureRequest request, String containerName) {
        store.delete(nsKey(request.accountName(), containerName));
        String objPrefix = request.accountName() + "/" + containerName + "/";
        String blkPrefix = BLK_PREFIX + objPrefix;
        store.keys().stream()
                .filter(k -> k.startsWith(objPrefix) || k.startsWith(blkPrefix))
                .toList()
                .forEach(store::delete);
        return Response.status(Response.Status.ACCEPTED).build();
    }

    private Response listContainers(AzureRequest request) {
        String prefix = request.queryParams().getOrDefault("prefix", "");
        String nsFilter = NS_PREFIX + request.accountName() + "/" + prefix;

        List<BlobModels.ContainerItem> containers = store.keys().stream()
                .filter(k -> k.startsWith(nsFilter))
                .map(k -> k.substring(NS_PREFIX.length() + request.accountName().length() + 1))
                .map(name -> new BlobModels.ContainerItem(name, new BlobModels.ContainerProperties(
                        RFC1123_DATE_TIME.format(Instant.now()),
                        UUID.randomUUID().toString()
                )))
                .collect(Collectors.toList());

        BlobModels.ContainerListResponse response = new BlobModels.ContainerListResponse(
                "http://localhost:4577/" + request.accountName(),
                prefix, "", 1000, containers, ""
        );

        return Response.ok(XmlUtils.toXml(response)).type(MediaType.APPLICATION_XML).build();
    }

    private Response putBlob(AzureRequest request, String containerName, String blobName) {
        try {
            if (store.get(nsKey(request.accountName(), containerName)).isEmpty()) {
                return new AzureErrorResponse("ContainerNotFound", "The specified container does not exist.")
                        .toXmlResponse(Response.Status.NOT_FOUND.getStatusCode());
            }

            Optional<StoredObject> existing = store.get(objKey(request.accountName(), containerName, blobName));
            Response conditionFailure = validateBlobConditions(request, existing);
            if (conditionFailure != null) {
                return conditionFailure;
            }

            if (hierarchicalNamespaceAccounts.contains(request.accountName())) {
                createParentDirectories(request.accountName(), containerName, blobName);
            }

            byte[] data = request.bodyStream().readAllBytes();
            Map<String, String> metadata = new HashMap<>();
            String blobType = request.headers().getHeaderString("x-ms-blob-type");
            metadata.put("BlobType", blobType != null ? blobType : "BlockBlob");
            String ct = request.headers().getHeaderString(HttpHeaders.CONTENT_TYPE);
            metadata.put("Content-Type", ct != null ? ct : "application/octet-stream");
            metadata.put("Name", blobName);
            metadata.putAll(readUserMetadata(request));

            String etag = UUID.randomUUID().toString();
            store.put(objKey(request.accountName(), containerName, blobName),
                    new StoredObject(blobName, data, metadata, Instant.now(), etag));

            return Response.status(Response.Status.CREATED)
                    .header("Last-Modified", RFC1123_DATE_TIME.format(Instant.now()))
                    .header("ETag", etag)
                    .header("x-ms-request-server-encrypted", "true")
                    .header("Content-Length", 0)
                    .build();
        } catch (IOException e) {
            return Response.serverError().build();
        }
    }

    private Response createDfsPath(AzureRequest request, String containerName, String blobName, boolean directory) {
        if (store.get(nsKey(request.accountName(), containerName)).isEmpty()) {
            return new AzureErrorResponse("ContainerNotFound", "The specified container does not exist.")
                    .toXmlResponse(Response.Status.NOT_FOUND.getStatusCode());
        }

        Optional<StoredObject> existing = store.get(objKey(request.accountName(), containerName, blobName));
        if (existing.isPresent() && "*".equals(request.headers().getHeaderString("If-None-Match"))) {
            return new AzureErrorResponse("PathAlreadyExists", "The specified path already exists.")
                    .toXmlResponse(Response.Status.CONFLICT.getStatusCode());
        }
        Response conditionFailure = validateBlobConditions(request, existing);
        if (conditionFailure != null) {
            return conditionFailure;
        }

        if (directory) {
            createParentDirectories(request.accountName(), containerName, blobName);
        }

        Map<String, String> metadata = new HashMap<>();
        metadata.put("BlobType", "BlockBlob");
        metadata.put("Content-Type", "application/octet-stream");
        metadata.put("Name", blobName);
        metadata.put("IsDirectory", Boolean.toString(directory));
        metadata.put(USER_METADATA_PREFIX + "hdi_isfolder", Boolean.toString(directory));
        String etag = UUID.randomUUID().toString();
        store.put(objKey(request.accountName(), containerName, blobName),
                new StoredObject(blobName, new byte[0], metadata, Instant.now(), etag));

        return Response.status(Response.Status.CREATED)
                .header("Last-Modified", RFC1123_DATE_TIME.format(Instant.now()))
                .header("ETag", etag)
                .header("x-ms-resource-type", directory ? "directory" : "file")
                .header("x-ms-meta-hdi_isfolder", Boolean.toString(directory))
                .header("x-ms-request-server-encrypted", "true")
                .header("Content-Length", 0)
                .build();
    }

    private static boolean isDfsPathCreate(AzureRequest request) {
        String resource = request.queryParams().get("resource");
        String resourceType = request.headers().getHeaderString("x-ms-resource-type");
        return "directory".equals(resource) || "file".equals(resource)
                || "directory".equals(resourceType) || "file".equals(resourceType);
    }

    private static boolean isDfsDirectoryCreate(AzureRequest request) {
        return "directory".equals(request.queryParams().get("resource"))
                || "directory".equals(request.headers().getHeaderString("x-ms-resource-type"));
    }

    private Response getBlob(AzureRequest request, String containerName, String blobName, boolean headOnly) {
        Optional<StoredObject> object = store.get(objKey(request.accountName(), containerName, blobName));

        if (object.isEmpty()) {
            return new AzureErrorResponse("BlobNotFound", "The specified blob does not exist.")
                    .toXmlResponse(Response.Status.NOT_FOUND.getStatusCode());
        }

        Response conditionFailure = validateBlobConditions(request, object);
        if (conditionFailure != null) {
            return conditionFailure;
        }

        StoredObject so = object.get();
        long totalSize = so.data().length;
        String rangeHeader = request.headers().getHeaderString("x-ms-range");
        if (rangeHeader == null) rangeHeader = request.headers().getHeaderString("Range");

        long rangeStart = 0;
        long rangeEnd   = totalSize - 1;
        boolean isRangeRequest = false;

        if (rangeHeader != null && rangeHeader.startsWith("bytes=")) {
            String[] parts = rangeHeader.substring(6).split("-", 2);
            try {
                rangeStart = Long.parseLong(parts[0]);
                rangeEnd   = parts.length > 1 && !parts[1].isEmpty()
                        ? Long.parseLong(parts[1]) : totalSize - 1;
                if (totalSize == 0 && rangeStart == 0 && rangeEnd == -1) {
                    return Response.ok(new byte[0])
                            .header("Last-Modified", RFC1123_DATE_TIME.format(so.lastModified()))
                            .header("ETag", so.etag())
                            .header("x-ms-blob-type", so.metadata().getOrDefault("BlobType", "BlockBlob"))
                            .header(HttpHeaders.CONTENT_TYPE, so.metadata().getOrDefault("Content-Type", "application/octet-stream"))
                            .header(HttpHeaders.CONTENT_LENGTH, 0)
                            .header("x-ms-blob-content-length", 0)
                            .header("Accept-Ranges", "bytes")
                            .build();
                }
                if (rangeStart < 0 || rangeStart >= totalSize) {
                    return Response.fromResponse(new AzureErrorResponse("InvalidRange",
                            "The range specified is invalid for the current size of the resource.")
                            .toXmlResponse(416))
                            .header("Content-Range", "bytes */" + totalSize)
                            .build();
                }
                rangeEnd   = Math.min(rangeEnd, totalSize - 1);
                isRangeRequest = true;
            } catch (NumberFormatException e) {
                return new AzureErrorResponse("InvalidRange",
                        "The range specified is invalid.").toXmlResponse(416);
            }
        }

        long contentLength = rangeEnd - rangeStart + 1;
        Response.ResponseBuilder rb = (isRangeRequest ? Response.status(206) : Response.ok())
                .header("Last-Modified", RFC1123_DATE_TIME.format(so.lastModified()))
                .header("ETag", so.etag())
                .header("x-ms-blob-type", so.metadata().getOrDefault("BlobType", "BlockBlob"))
                .header("x-ms-resource-type", Boolean.parseBoolean(so.metadata().getOrDefault("IsDirectory", "false")) ? "directory" : "file")
                .header(HttpHeaders.CONTENT_TYPE, so.metadata().getOrDefault("Content-Type", "application/octet-stream"))
                .header(HttpHeaders.CONTENT_LENGTH, contentLength)
                .header("Content-Range", String.format("bytes %d-%d/%d", rangeStart, rangeEnd, totalSize))
                .header("x-ms-blob-content-length", totalSize)
                .header("Accept-Ranges", "bytes");
        addUserMetadataHeaders(rb, so.metadata());

        if (!headOnly) {
            if (isRangeRequest) {
                // cast is safe: rangeStart/rangeEnd validated < totalSize which is bounded by int (byte[] length)
                rb.entity(Arrays.copyOfRange(so.data(), Math.toIntExact(rangeStart), Math.toIntExact(rangeEnd) + 1));
            } else {
                rb.entity(so.data());
            }
        }

        return rb.build();
    }

    private Response deleteBlob(AzureRequest request, String containerName, String blobName) {
        Optional<StoredObject> object = store.get(objKey(request.accountName(), containerName, blobName));
        if (object.isEmpty()) {
            return new AzureErrorResponse("BlobNotFound", "The specified blob does not exist.")
                    .toXmlResponse(Response.Status.NOT_FOUND.getStatusCode());
        }
        Response conditionFailure = validateBlobConditions(request, object);
        if (conditionFailure != null) {
            return conditionFailure;
        }
        if (Boolean.parseBoolean(object.orElseThrow().metadata().getOrDefault("IsDirectory", "false"))
                && Boolean.parseBoolean(request.queryParams().getOrDefault("recursive", "false"))) {
            String prefix = objKey(request.accountName(), containerName, blobName + "/");
            store.keys().stream()
                    .filter(key -> key.startsWith(prefix))
                    .toList()
                    .forEach(store::delete);
        }
        store.delete(objKey(request.accountName(), containerName, blobName));
        return Response.status(Response.Status.ACCEPTED).build();
    }

    private Response getBlobMetadata(AzureRequest request, String containerName, String blobName) {
        Optional<StoredObject> object = store.get(objKey(request.accountName(), containerName, blobName));
        if (object.isEmpty()) {
            return new AzureErrorResponse("BlobNotFound", "The specified blob does not exist.")
                    .toXmlResponse(Response.Status.NOT_FOUND.getStatusCode());
        }

        Response conditionFailure = validateBlobConditions(request, object);
        if (conditionFailure != null) {
            return conditionFailure;
        }

        StoredObject so = object.get();
        Response.ResponseBuilder rb = Response.ok()
                .header("Last-Modified", RFC1123_DATE_TIME.format(so.lastModified()))
                .header("ETag", so.etag());
        addUserMetadataHeaders(rb, so.metadata());
        return rb.build();
    }

    private Response setBlobMetadata(AzureRequest request, String containerName, String blobName) {
        Optional<StoredObject> object = store.get(objKey(request.accountName(), containerName, blobName));
        if (object.isEmpty()) {
            return new AzureErrorResponse("BlobNotFound", "The specified blob does not exist.")
                    .toXmlResponse(Response.Status.NOT_FOUND.getStatusCode());
        }

        Response conditionFailure = validateBlobConditions(request, object);
        if (conditionFailure != null) {
            return conditionFailure;
        }

        StoredObject so = object.get();
        Map<String, String> metadata = new HashMap<>();
        so.metadata().forEach((key, value) -> {
            if (!key.startsWith(USER_METADATA_PREFIX)) {
                metadata.put(key, value);
            }
        });
        metadata.putAll(readUserMetadata(request));

        String etag = UUID.randomUUID().toString();
        store.put(objKey(request.accountName(), containerName, blobName),
                new StoredObject(so.key(), so.data(), metadata, Instant.now(), etag));

        return Response.ok()
                .header("Last-Modified", RFC1123_DATE_TIME.format(Instant.now()))
                .header("ETag", etag)
                .build();
    }

    private Response listBlobs(AzureRequest request, String containerName) {
        String prefix = request.queryParams().getOrDefault("prefix", "");
        String delimiter = request.queryParams().getOrDefault("delimiter", "");
        String marker = request.queryParams().getOrDefault("marker", "");
        String startFrom = request.queryParams().getOrDefault("startFrom", "");
        int maxResults = parseMaxResults(request.queryParams().get("maxresults"));
        String keyPrefix = objKey(request.accountName(), containerName, prefix);

        List<BlobModels.BlobPrefix> blobPrefixes = new ArrayList<>();
        List<BlobModels.BlobItem> blobs = new ArrayList<>();
        Set<String> seenPrefixes = new HashSet<>();
        store.scan(k -> k.startsWith(keyPrefix)).forEach(so -> {
            String name = so.metadata().getOrDefault("Name", so.key());
            if (!delimiter.isEmpty()) {
                String remaining = name.substring(prefix.length());
                int delimiterIndex = remaining.indexOf(delimiter);
                if (delimiterIndex >= 0) {
                    String blobPrefix = name.substring(0, prefix.length() + delimiterIndex + delimiter.length());
                    if (seenPrefixes.add(blobPrefix)) {
                        blobPrefixes.add(new BlobModels.BlobPrefix(blobPrefix));
                    }
                    return;
                }
            }
            blobs.add(new BlobModels.BlobItem(name, new BlobModels.BlobProperties(
                    RFC1123_DATE_TIME.format(so.lastModified()),
                    so.etag(),
                    (long) so.data().length,
                    so.metadata().getOrDefault("Content-Type", "application/octet-stream"),
                    so.metadata().getOrDefault("BlobType", "BlockBlob")
            ), includes(request.queryParams().get("include"), "metadata") ? userMetadata(so.metadata()) : null));
        });
        blobPrefixes.sort(Comparator.comparing(BlobModels.BlobPrefix::Name));
        blobs.sort(Comparator.comparing(BlobModels.BlobItem::Name));

        int start = 0;
        String lowerBound = !marker.isEmpty() ? marker : startFrom;
        if (!lowerBound.isEmpty()) {
            while (start < blobs.size() && blobs.get(start).Name().compareTo(lowerBound) < 0) {
                start++;
            }
        }
        int end = Math.min(start + maxResults, blobs.size());
        String nextMarker = end < blobs.size() ? blobs.get(end).Name() : "";

        BlobModels.BlobListResponse response = new BlobModels.BlobListResponse(
                "http://localhost:4577/" + request.accountName(),
                containerName, prefix, delimiter, marker, maxResults, new BlobModels.BlobItems(blobPrefixes, blobs.subList(start, end)), nextMarker
        );

        return Response.ok(XmlUtils.toXml(response)).type(MediaType.APPLICATION_XML).build();
    }

    private Response renameDfsPath(AzureRequest request, String targetContainerName, String targetBlobName) {
        String source = request.headers().getHeaderString("x-ms-rename-source");
        if (source == null || !source.startsWith("/")) {
            return new AzureErrorResponse("InvalidSourceUri", "The source URI is invalid.").toXmlResponse(400);
        }

        String[] sourceParts = source.substring(1).split("/", 2);
        if (sourceParts.length != 2) {
            return new AzureErrorResponse("InvalidSourceUri", "The source URI is invalid.").toXmlResponse(400);
        }

        String sourceContainerName = sourceParts[0];
        String sourceBlobName = java.net.URLDecoder.decode(sourceParts[1], StandardCharsets.UTF_8);
        Optional<String> normalizedSourceBlobName = normalizePath(sourceBlobName);
        if (normalizedSourceBlobName.isEmpty()) {
            return new AzureErrorResponse("InvalidQueryParameterValue",
                    "Value for one of the query parameters specified in the request URI is invalid.")
                    .toXmlResponse(400);
        }
        sourceBlobName = normalizedSourceBlobName.orElseThrow();

        Optional<StoredObject> sourceObject = store.get(objKey(request.accountName(), sourceContainerName, sourceBlobName));
        if (sourceObject.isEmpty()) {
            return new AzureErrorResponse("BlobNotFound", "The specified blob does not exist.")
                    .toXmlResponse(Response.Status.NOT_FOUND.getStatusCode());
        }
        if (!sourceContainerName.equals(targetContainerName)) {
            return new AzureErrorResponse("InvalidSourceUri", "The source URI is invalid.").toXmlResponse(400);
        }
        if (store.get(objKey(request.accountName(), targetContainerName, targetBlobName)).isPresent()) {
            return new AzureErrorResponse("PathAlreadyExists", "The specified path already exists.")
                    .toXmlResponse(Response.Status.PRECONDITION_FAILED.getStatusCode());
        }

        StoredObject so = sourceObject.orElseThrow();
        Map<String, String> metadata = new HashMap<>(so.metadata());
        metadata.put("Name", targetBlobName);
        String etag = UUID.randomUUID().toString();
        store.put(objKey(request.accountName(), targetContainerName, targetBlobName),
                new StoredObject(targetBlobName, so.data(), metadata, Instant.now(), etag));
        store.delete(objKey(request.accountName(), sourceContainerName, sourceBlobName));
        if (Boolean.parseBoolean(so.metadata().getOrDefault("IsDirectory", "false"))) {
            String sourcePrefix = objKey(request.accountName(), sourceContainerName, sourceBlobName + "/");
            List<StoredObject> children = store.scan(key -> key.startsWith(sourcePrefix));
            for (StoredObject child : children) {
                String childName = child.metadata().getOrDefault("Name", child.key());
                String targetChildName = targetBlobName + childName.substring(sourceBlobName.length());
                Map<String, String> childMetadata = new HashMap<>(child.metadata());
                childMetadata.put("Name", targetChildName);
                store.put(objKey(request.accountName(), targetContainerName, targetChildName),
                        new StoredObject(targetChildName, child.data(), childMetadata, Instant.now(), UUID.randomUUID().toString()));
                store.delete(objKey(request.accountName(), sourceContainerName, childName));
            }
        }

        return Response.status(Response.Status.CREATED)
                .header("ETag", etag)
                .header("Last-Modified", RFC1123_DATE_TIME.format(Instant.now()))
                .build();
    }

    private void createParentDirectories(String accountName, String containerName, String blobName) {
        int separator = blobName.indexOf('/');
        while (separator >= 0) {
            String directoryName = blobName.substring(0, separator);
            String key = objKey(accountName, containerName, directoryName);
            if (store.get(key).isEmpty()) {
                Map<String, String> metadata = new HashMap<>();
                metadata.put("BlobType", "BlockBlob");
                metadata.put("Content-Type", "application/octet-stream");
                metadata.put("Name", directoryName);
                metadata.put("IsDirectory", "true");
                metadata.put(USER_METADATA_PREFIX + "hdi_isfolder", "true");
                store.put(key, new StoredObject(directoryName, new byte[0], metadata, Instant.now(), UUID.randomUUID().toString()));
            }
            separator = blobName.indexOf('/', separator + 1);
        }
    }

    private Response listDfsPaths(AzureRequest request, String containerName) {
        return listDfsPaths(request, containerName, "");
    }

    private Response listDfsPaths(AzureRequest request, String containerName, String defaultDirectory) {
        String directory = request.queryParams().get("directory");
        if (directory == null || directory.isEmpty()) {
            directory = defaultDirectory;
        }
        if (!directory.isEmpty()) {
            Optional<String> normalizedDirectory = normalizePath(directory);
            if (normalizedDirectory.isEmpty()) {
                return new AzureErrorResponse("InvalidQueryParameterValue",
                        "Value for one of the query parameters specified in the request URI is invalid.")
                        .toXmlResponse(400);
            }
            directory = normalizedDirectory.orElseThrow();
        }
        boolean recursive = Boolean.parseBoolean(request.queryParams().getOrDefault("recursive", "false"));
        String containerPrefix = objKey(request.accountName(), containerName, "");
        String directoryPrefix = directory.isEmpty() ? "" : directory + "/";
        String listedDirectory = directory;

        Map<String, DfsPath> paths = new HashMap<>();
        store.scan(key -> key.startsWith(containerPrefix)).forEach(object -> {
            String blobName = object.key();
            if (!directoryPrefix.isEmpty() && !blobName.startsWith(directoryPrefix)) {
                return;
            }
            String relativeName = directoryPrefix.isEmpty() ? blobName : blobName.substring(directoryPrefix.length());
            if (relativeName.isEmpty()) {
                return;
            }

            int separator = relativeName.indexOf('/');
            if (!recursive && separator >= 0) {
                String childDirectory = directoryPrefix + relativeName.substring(0, separator);
                paths.putIfAbsent(childDirectory, new DfsPath(childDirectory, true, 0, object.lastModified(), object.etag()));
                return;
            }

            boolean directoryPath = Boolean.parseBoolean(object.metadata().getOrDefault("IsDirectory", "false"));
            addParentDirectories(paths, blobName, listedDirectory, object.lastModified(), object.etag());
            paths.put(blobName, new DfsPath(blobName, directoryPath, directoryPath ? 0 : object.data().length, object.lastModified(), object.etag()));
        });
        if (!directory.isEmpty()) {
            paths.remove(directory);
        }

        String pathsJson = paths.values().stream()
                .sorted(Comparator.comparing(DfsPath::name))
                .map(BlobServiceHandler::pathJson)
                .collect(Collectors.joining(","));
        return Response.ok("{\"paths\":[" + pathsJson + "]}")
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

    private static void addParentDirectories(Map<String, DfsPath> paths, String blobName, String directory, Instant lastModified, String etag) {
        int separator = blobName.indexOf('/');
        String directoryPrefix = directory.isEmpty() ? "" : directory + "/";
        while (separator >= 0) {
            String parentDirectory = blobName.substring(0, separator);
            if (directory.isEmpty() || parentDirectory.startsWith(directoryPrefix)) {
                paths.putIfAbsent(parentDirectory, new DfsPath(parentDirectory, true, 0, lastModified, etag));
            }
            separator = blobName.indexOf('/', separator + 1);
        }
    }

    private static String pathJson(DfsPath path) {
        return "{"
                + "\"contentLength\":\"" + path.contentLength() + "\","
                + "\"etag\":\"" + path.etag() + "\","
                + "\"group\":\"$superuser\","
                + "\"isDirectory\":\"" + path.directory() + "\","
                + "\"lastModified\":\"" + RFC1123_DATE_TIME.format(path.lastModified()) + "\","
                + "\"name\":\"" + path.name() + "\","
                + "\"owner\":\"$superuser\","
                + "\"permissions\":\"" + (path.directory() ? "rwxr-x---" : "rw-r-----") + "\""
                + "}";
    }

    private record DfsPath(String name, boolean directory, long contentLength, Instant lastModified, String etag) {}

    private static int parseMaxResults(String value) {
        if (value == null || value.isBlank()) {
            return 1000;
        }
        return Integer.parseInt(value);
    }

    // ── Block Blob ────────────────────────────────────────────────────────────

    /**
     * PUT /{container}/{blob}?comp=block&blockid={BASE64}
     * <p>Stages one block. Data is stored under a {@code __blk__:} key and only
     * becomes part of the blob after a successful {@link #putBlockList}.
     */
    private Response putBlock(AzureRequest request, String containerName, String blobName) {
        try {
            if (store.get(nsKey(request.accountName(), containerName)).isEmpty()) {
                return new AzureErrorResponse("ContainerNotFound", "The specified container does not exist.")
                        .toXmlResponse(Response.Status.NOT_FOUND.getStatusCode());
            }
            String blockId = request.queryParams().get("blockid");
            if (blockId == null || blockId.isBlank()) {
                return new AzureErrorResponse("InvalidQueryParameterValue",
                        "Value for one of the query parameters specified in the request URI is invalid.")
                        .toXmlResponse(400);
            }
            byte[] data = request.bodyStream().readAllBytes();
            store.put(blockStagingKey(request.accountName(), containerName, blobName, blockId),
                    new StoredObject(blockId, data, Map.of("BlockId", blockId), Instant.now(),
                            UUID.randomUUID().toString()));
            return Response.status(Response.Status.CREATED)
                    .header("x-ms-request-server-encrypted", "true")
                    .header("Content-Length", 0)
                    .build();
        } catch (IOException e) {
            LOGGER.errorf(e, "putBlock I/O error: container=%s blob=%s", containerName, blobName);
            return Response.serverError().build();
        }
    }

    /**
     * PUT /{container}/{blob}?comp=blocklist
     * <p>Commits an ordered list of previously-staged blocks into a blob.
     * After a successful commit, all staged blocks for this blob are deleted.
     */
    private Response putBlockList(AzureRequest request, String containerName, String blobName) {
        try {
            if (store.get(nsKey(request.accountName(), containerName)).isEmpty()) {
                return new AzureErrorResponse("ContainerNotFound", "The specified container does not exist.")
                        .toXmlResponse(Response.Status.NOT_FOUND.getStatusCode());
            }

            List<String> blockIds = parseBlockList(request.bodyStream().readAllBytes());

            // Resolve every block ID → staged data
            List<byte[]> chunks = new ArrayList<>(blockIds.size());
            List<String> committedMeta = new ArrayList<>(blockIds.size()); // "base64id:size"

            for (String blockId : blockIds) {
                Optional<StoredObject> staged = store.get(
                        blockStagingKey(request.accountName(), containerName, blobName, blockId));
                if (staged.isEmpty()) {
                    return new AzureErrorResponse("InvalidBlockList",
                            "The specified block list is invalid.")
                            .toXmlResponse(400);
                }
                byte[] blockData = staged.get().data();
                chunks.add(blockData);
                committedMeta.add(blockId + ":" + blockData.length);
            }

            // Concatenate all block data into the final blob body
            int totalSize = chunks.stream().mapToInt(c -> c.length).sum();
            byte[] assembled = new byte[totalSize];
            int offset = 0;
            for (byte[] chunk : chunks) {
                System.arraycopy(chunk, 0, assembled, offset, chunk.length);
                offset += chunk.length;
            }

            // Build blob metadata
            Map<String, String> metadata = new HashMap<>();
            String blobType = request.headers().getHeaderString("x-ms-blob-type");
            metadata.put("BlobType", blobType != null ? blobType : "BlockBlob");
            String ct = request.headers().getHeaderString(HttpHeaders.CONTENT_TYPE);
            metadata.put("Content-Type", ct != null ? ct : "application/octet-stream");
            metadata.put("Name", blobName);
            // Persist committed block list for future GetBlockList calls
            metadata.put("CommittedBlocks", String.join("|", committedMeta));
            metadata.putAll(readUserMetadata(request));

            String etag = UUID.randomUUID().toString();
            store.put(objKey(request.accountName(), containerName, blobName),
                    new StoredObject(blobName, assembled, metadata, Instant.now(), etag));

            // Clean up all staged blocks for this blob
            String stagePrefix = blockStagingPrefix(request.accountName(), containerName, blobName);
            store.keys().stream()
                    .filter(k -> k.startsWith(stagePrefix))
                    .toList()
                    .forEach(store::delete);

            return Response.status(Response.Status.CREATED)
                    .header("Last-Modified", RFC1123_DATE_TIME.format(Instant.now()))
                    .header("ETag", etag)
                    .header("x-ms-request-server-encrypted", "true")
                    .header("Content-Length", 0)
                    .build();
        } catch (IOException e) {
            LOGGER.errorf(e, "putBlockList I/O error: container=%s blob=%s", containerName, blobName);
            return Response.serverError().build();
        }
    }

    /**
     * GET /{container}/{blob}?comp=blocklist[&blocklisttype=committed|uncommitted|all]
     * <p>Returns committed blocks (from blob metadata) and/or uncommitted
     * (staged) blocks, depending on {@code blocklisttype}.
     */
    private Response getBlockList(AzureRequest request, String containerName, String blobName) {
        String listType = request.queryParams().getOrDefault("blocklisttype", "committed");

        List<BlobModels.BlockItem> committed   = new ArrayList<>();
        List<BlobModels.BlockItem> uncommitted = new ArrayList<>();

        if ("committed".equals(listType) || "all".equals(listType)) {
            store.get(objKey(request.accountName(), containerName, blobName))
                 .ifPresent(blob -> {
                     String meta = blob.metadata().getOrDefault("CommittedBlocks", "");
                     if (!meta.isBlank()) {
                         for (String entry : meta.split("\\|")) {
                             String[] parts = entry.split(":", 2);
                             if (parts.length == 2) {
                                 try {
                                     committed.add(new BlobModels.BlockItem(parts[0], Long.parseLong(parts[1])));
                                 } catch (NumberFormatException ignored) {
                                     // corrupt entry — skip
                                 }
                             }
                         }
                     }
                 });
        }

        if ("uncommitted".equals(listType) || "all".equals(listType)) {
            String stagePrefix = blockStagingPrefix(request.accountName(), containerName, blobName);
            store.scan(k -> k.startsWith(stagePrefix)).stream()
                 .map(so -> new BlobModels.BlockItem(so.key(), (long) so.data().length))
                 .forEach(uncommitted::add);
        }

        String body = buildBlockListXml(committed, uncommitted);
        return Response.ok(body).type(MediaType.APPLICATION_XML).build();
    }

    private static String buildBlockListXml(List<BlobModels.BlockItem> committed,
                                            List<BlobModels.BlockItem> uncommitted) {
        XmlBuilder xml = new XmlBuilder()
                .start("BlockList")
                .start("CommittedBlocks");
        appendBlockItems(xml, committed);
        xml.end("CommittedBlocks")
                .start("UncommittedBlocks");
        appendBlockItems(xml, uncommitted);
        return xml.end("UncommittedBlocks")
                .end("BlockList")
                .build();
    }

    private static void appendBlockItems(XmlBuilder xml, List<BlobModels.BlockItem> blocks) {
        for (BlobModels.BlockItem block : blocks) {
            xml.start("Block")
                    .elem("Name", block.Name())
                    .elem("Size", block.Size())
                    .end("Block");
        }
    }

    // ── Block key helpers ─────────────────────────────────────────────────────

    /**
     * Storage key for a single staged block.
     * Format: {@code __blk__:account/container/blobName:blockId}
     * <p>{@code :} is safe as separator — blockIds are Base64 ({@code [A-Za-z0-9+/=]}).
     */
    private static String blockStagingKey(String account, String container,
                                           String blobName, String blockId) {
        return BLK_PREFIX + objKey(account, container, blobName) + ":" + blockId;
    }

    /** Prefix that matches all staged blocks for a given blob. */
    private static String blockStagingPrefix(String account, String container, String blobName) {
        return BLK_PREFIX + objKey(account, container, blobName) + ":";
    }

    /**
     * Parses the block IDs from a PutBlockList XML body.
     * Matches {@code <Latest>}, {@code <Committed>}, and {@code <Uncommitted>} elements
     * in document order — Azure treats all three as "use this block".
     */
    private static List<String> parseBlockList(byte[] body) {
        String xml = new String(body, StandardCharsets.UTF_8);
        List<String> ids = new ArrayList<>();
        Matcher m = BLOCK_LIST_PATTERN.matcher(xml);
        while (m.find()) {
            ids.add(m.group(1).trim());
        }
        return ids;
    }

    public void clearAll() {
        store.clear();
    }

    public void ensureContainer(String accountName, String containerName) {
        store.put(nsKey(accountName, containerName), NS_SENTINEL);
    }

    private static String nsKey(String accountName, String containerName) {
        return NS_PREFIX + accountName + "/" + containerName;
    }

    private static String objKey(String accountName, String containerName, String blobName) {
        return accountName + "/" + containerName + "/" + blobName;
    }

    private static Optional<String> normalizePath(String path) {
        Deque<String> segments = new ArrayDeque<>();
        for (String segment : path.split("/")) {
            if (segment.isEmpty() || ".".equals(segment)) {
                continue;
            }
            if ("..".equals(segment)) {
                if (segments.isEmpty()) {
                    return Optional.empty();
                }
                segments.removeLast();
                continue;
            }
            segments.addLast(segment);
        }
        return Optional.of(String.join("/", segments));
    }

    private static Map<String, String> readUserMetadata(AzureRequest request) {
        Map<String, String> metadata = new HashMap<>();
        request.headers().getRequestHeaders().forEach((name, values) -> {
            if (name.toLowerCase(Locale.ROOT).startsWith("x-ms-meta-") && !values.isEmpty()) {
                metadata.put(USER_METADATA_PREFIX + name.substring("x-ms-meta-".length()).toLowerCase(Locale.ROOT),
                        values.get(0));
            }
        });
        return metadata;
    }

    private static Map<String, String> userMetadata(Map<String, String> storedMetadata) {
        return storedMetadata.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(USER_METADATA_PREFIX))
                .collect(Collectors.toMap(
                        entry -> entry.getKey().substring(USER_METADATA_PREFIX.length()),
                        Map.Entry::getValue,
                        (left, right) -> right,
                        LinkedHashMap::new
                ));
    }

    private static void addUserMetadataHeaders(Response.ResponseBuilder rb, Map<String, String> storedMetadata) {
        userMetadata(storedMetadata).forEach((key, value) -> rb.header("x-ms-meta-" + key, value));
    }

    private static boolean includes(String include, String value) {
        if (include == null || include.isBlank()) {
            return false;
        }
        return Arrays.stream(include.split(","))
                .map(String::trim)
                .anyMatch(value::equalsIgnoreCase);
    }

    private static Response validateBlobConditions(AzureRequest request, Optional<StoredObject> object) {
        String ifMatch = request.headers().getHeaderString(HttpHeaders.IF_MATCH);
        if (ifMatch != null && object.map(StoredObject::etag).filter(etag -> etagMatches(ifMatch, etag)).isEmpty()) {
            return new AzureErrorResponse("ConditionNotMet", "The condition specified using HTTP conditional header(s) is not met.")
                    .toXmlResponse(Response.Status.PRECONDITION_FAILED.getStatusCode());
        }

        String ifNoneMatch = request.headers().getHeaderString(HttpHeaders.IF_NONE_MATCH);
        if (ifNoneMatch != null && object.map(StoredObject::etag).filter(etag -> etagMatches(ifNoneMatch, etag)).isPresent()) {
            return new AzureErrorResponse("ConditionNotMet", "The condition specified using HTTP conditional header(s) is not met.")
                    .toXmlResponse(Response.Status.PRECONDITION_FAILED.getStatusCode());
        }
        return null;
    }

    private static boolean etagMatches(String condition, String etag) {
        if ("*".equals(condition.trim())) {
            return true;
        }
        return Arrays.stream(condition.split(","))
                .map(String::trim)
                .map(BlobServiceHandler::unquote)
                .anyMatch(candidate -> candidate.equals(unquote(etag)));
    }

    private static String unquote(String value) {
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
