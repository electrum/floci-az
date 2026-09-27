package io.floci.az.services.arm;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.restassured.RestAssured.given;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@DisplayName("ARM storage accounts")
class ArmStorageAccountTest {

    @Test
    void storageAccountNamesAreUniqueAcrossResourceGroups() {
        given()
            .contentType("application/json")
            .body("{\"location\":\"eastus\"}")
            .when().put("/subscriptions/sub-unique/resourceGroups/rg-first"
                    + "/providers/Microsoft.Storage/storageAccounts/uniquestorageaccount?api-version=2023-01-01")
            .then()
            .statusCode(200);

        given()
            .contentType("application/json")
            .body("{\"location\":\"eastus\"}")
            .when().put("/subscriptions/sub-unique/resourceGroups/rg-second"
                    + "/providers/Microsoft.Storage/storageAccounts/uniquestorageaccount?api-version=2023-01-01")
            .then()
            .statusCode(409)
            .body("error.code", equalTo("StorageAccountAlreadyExists"))
            .body("error.message", equalTo("The storage account named uniquestorageaccount already exists under the subscription."));

        given()
            .contentType("application/json")
            .body("{\"location\":\"eastus\"}")
            .when().put("/subscriptions/sub-other/resourceGroups/rg-other"
                    + "/providers/Microsoft.Storage/storageAccounts/uniquestorageaccount?api-version=2023-01-01")
            .then()
            .statusCode(409)
            .body("error.code", equalTo("StorageAccountAlreadyTaken"))
            .body("error.message", equalTo("The storage account named uniquestorageaccount is already taken."));
    }

    @Test
    void concurrentStorageAccountCreationClaimsNameOnce()
            throws Exception {
        int requestCount = 16;
        CountDownLatch ready = new CountDownLatch(requestCount);
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(requestCount);
        try {
            List<Future<Integer>> requests = range(0, requestCount)
                    .mapToObj(index -> executor.submit(() -> {
                        ready.countDown();
                        start.await();
                        return given()
                                .contentType("application/json")
                                .body("{\"location\":\"eastus\"}")
                                .when().put("/subscriptions/sub-concurrent/resourceGroups/rg-" + index
                                        + "/providers/Microsoft.Storage/storageAccounts/concurrentaccount?api-version=2023-01-01")
                                .statusCode();
                    }))
                    .toList();

            assertTrue(ready.await(10, SECONDS));
            start.countDown();

            int successCount = 0;
            for (Future<Integer> request : requests) {
                int status = request.get(10, SECONDS);
                assertTrue(status == 200 || status == 409, "Unexpected response status: " + status);
                if (status == 200) {
                    successCount++;
                }
            }
            assertEquals(1, successCount);
        }
        finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void storageAccountPrimaryEndpointsIncludeDfs() {
        given()
            .contentType("application/json")
            .body("{\"location\":\"eastus\"}")
            .when().put("/subscriptions/sub-cred-vending/resourceGroups/rg-cred-vending"
                    + "/providers/Microsoft.Storage/storageAccounts/credvendingacct?api-version=2023-01-01")
            .then()
            .statusCode(200)
            .body("properties.primaryEndpoints.blob",
                    equalTo("http://credvendingacct.blob.core.windows.net/"))
            .body("properties.primaryEndpoints.dfs",
                    equalTo("http://credvendingacct.dfs.core.windows.net/"));
    }
}
