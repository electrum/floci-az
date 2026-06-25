package io.floci.az.compat;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.UUID;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Data Lake Storage Compatibility")
class DataLakeCompatibilityTest {

    private DataLakeServiceClient client;
    private DataLakeServiceClient pathBasedClient;

    @BeforeAll
    void setup() {
        EmulatorConfig.assumeEmulatorRunning();
        client = new DataLakeServiceClientBuilder()
                .endpoint(EmulatorConfig.httpBase())
                .credential(new StorageSharedKeyCredential(EmulatorConfig.ACCOUNT, EmulatorConfig.DEV_KEY))
                .addPolicy((context, next) -> {
                    context.getHttpRequest().setHeader("Host", EmulatorConfig.ACCOUNT + ".dfs.core.windows.net");
                    return next.process();
                })
                .buildClient();
        pathBasedClient = new DataLakeServiceClientBuilder()
                .endpoint(EmulatorConfig.httpBase() + "/" + EmulatorConfig.ACCOUNT)
                .credential(new StorageSharedKeyCredential(EmulatorConfig.ACCOUNT, EmulatorConfig.DEV_KEY))
                .buildClient();
    }

    @Test
    @DisplayName("file create: creates path through dfs endpoint")
    void fileCreateUsesDfsEndpoint() {
        String name = "test-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        DataLakeFileSystemClient fileSystem = client.createFileSystem(name);

        DataLakeFileClient file = fileSystem.createFile("dir/file.txt");

        assertTrue(file.exists());

        client.deleteFileSystem(name);
    }

    @Test
    @DisplayName("path list: returns files and synthesized parent directories")
    void pathListReturnsFilesAndParentDirectories() {
        String name = "test-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        DataLakeFileSystemClient fileSystem = client.createFileSystem(name);
        fileSystem.createFile("dir/file.txt");

        var paths = fileSystem.listPaths(new ListPathsOptions().setRecursive(true), null).stream()
                .collect(toSet());

        assertTrue(paths.stream().anyMatch(path -> path.getName().equals("dir") && path.isDirectory()));
        assertTrue(paths.stream().anyMatch(path -> path.getName().equals("dir/file.txt") && !path.isDirectory()));

        client.deleteFileSystem(name);
    }

    @Test
    @DisplayName("path list: scoped listing returns directory children")
    void pathListScopedListingReturnsDirectoryChildren() {
        String name = "test-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        DataLakeFileSystemClient fileSystem = client.createFileSystem(name);
        fileSystem.createFile("dir/child/file.txt");

        var paths = fileSystem.getDirectoryClient("dir").listPaths(true, false, null, null).stream()
                .collect(toSet());

        assertTrue(paths.stream().anyMatch(path -> path.getName().equals("dir/child") && path.isDirectory()));
        assertTrue(paths.stream().anyMatch(path -> path.getName().equals("dir/child/file.txt") && !path.isDirectory()));
        assertFalse(paths.stream().anyMatch(path -> path.getName().equals("dir")));

        client.deleteFileSystem(name);
    }

    @Test
    @DisplayName("directory create: properties identify directories")
    void directoryCreatePropertiesIdentifyDirectories() {
        String name = "test-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        DataLakeFileSystemClient fileSystem = client.createFileSystem(name);

        assertTrue(fileSystem.createDirectoryIfNotExists("level0/level1/level2").getProperties().isDirectory());

        client.deleteFileSystem(name);
    }

    @Test
    @DisplayName("path-based endpoint: directory properties identify directories")
    void pathBasedEndpointDirectoryPropertiesIdentifyDirectories() {
        String name = "test-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        DataLakeServiceClient hnsClient = new DataLakeServiceClientBuilder()
                .endpoint(EmulatorConfig.httpBase() + "/devstoreaccount1hns")
                .credential(new StorageSharedKeyCredential("devstoreaccount1hns", EmulatorConfig.DEV_KEY))
                .buildClient();
        DataLakeFileSystemClient fileSystem = hnsClient.createFileSystem(name);

        assertTrue(fileSystem.createDirectoryIfNotExists("level0/level1/level2").getProperties().isDirectory());

        hnsClient.deleteFileSystem(name);
    }

    @Test
    @DisplayName("file rename: moves source to target")
    void fileRenameMovesSourceToTarget() {
        String name = "test-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        DataLakeFileSystemClient fileSystem = client.createFileSystem(name);
        DataLakeFileClient source = fileSystem.createFile("rename/source.txt");

        source.renameWithResponse(null, "rename/target.txt", null, null, null, null);

        assertFalse(source.exists());
        assertTrue(fileSystem.getFileClient("rename/target.txt").exists());

        client.deleteFileSystem(name);
    }

    @Test
    @DisplayName("path normalization: resolves aliases and rejects escaping root")
    void pathNormalizationResolvesAliasesAndRejectsEscapingRoot() {
        String name = "test-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        DataLakeFileSystemClient fileSystem = client.createFileSystem(name);
        fileSystem.createFile("b");

        assertTrue(fileSystem.getFileClient("a/../b").exists());
        DataLakeStorageException exception = assertThrows(DataLakeStorageException.class,
                () -> fileSystem.createFile("../outside.txt"));
        assertEquals(400, exception.getStatusCode());

        client.deleteFileSystem(name);
    }
}
