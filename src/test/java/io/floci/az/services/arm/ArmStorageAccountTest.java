package io.floci.az.services.arm;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

@QuarkusTest
@DisplayName("ARM storage accounts")
class ArmStorageAccountTest {

    @Test
    void storageAccountNameAvailabilityReflectsExistingAccounts() {
        String account = "availabilityaccount";
        String availabilityPath = "/subscriptions/sub-availability/providers/Microsoft.Storage"
                + "/checkNameAvailability?api-version=2023-01-01";
        String requestBody = "{\"name\":\"" + account + "\",\"type\":\"Microsoft.Storage/storageAccounts\"}";

        given()
            .contentType("application/json")
            .body(requestBody)
            .when().post(availabilityPath)
            .then()
            .statusCode(200)
            .body("nameAvailable", equalTo(true));

        given()
            .contentType("application/json")
            .body("{\"location\":\"eastus\"}")
            .when().put("/subscriptions/sub-availability/resourceGroups/rg-availability"
                    + "/providers/Microsoft.Storage/storageAccounts/" + account + "?api-version=2023-01-01")
            .then()
            .statusCode(200);

        given()
            .contentType("application/json")
            .body(requestBody)
            .when().post(availabilityPath)
            .then()
            .statusCode(200)
            .body("nameAvailable", equalTo(false))
            .body("reason", equalTo("AlreadyExists"))
            .body("message", equalTo("The storage account named " + account + " is already taken."));
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
