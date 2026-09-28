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

    @Test
    void invalidStorageAccountNameIsReportedUnavailable() {
        given()
            .contentType("application/json")
            .body("{\"location\":\"eastus\"}")
            .when().put("/subscriptions/sub-name/resourceGroups/rg-name"
                    + "/providers/Microsoft.Storage/storageAccounts/flociupper1703?api-version=2023-01-01")
            .then()
            .statusCode(200);

        given()
            .contentType("application/json")
            .body("{\"name\":\"FlociUpper1703\",\"type\":\"Microsoft.Storage/storageAccounts\"}")
            .when().post("/subscriptions/sub-name/providers/Microsoft.Storage/checkNameAvailability"
                    + "?api-version=2023-01-01")
            .then()
            .statusCode(200)
            .body("nameAvailable", equalTo(false))
            .body("reason", equalTo("AccountNameInvalid"))
            .body("message", equalTo("FlociUpper1703 is not a valid storage account name. Storage account name "
                    + "must be between 3 and 24 characters in length and use numbers and lower-case letters only."));
    }
}
