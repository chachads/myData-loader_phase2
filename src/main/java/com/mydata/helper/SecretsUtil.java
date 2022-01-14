package com.mydata.helper;

/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/


import org.json.JSONObject;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

/**
 * Utility class for handling a secret from Secrets Manager.
 */
public abstract class SecretsUtil {

    /**
     * Retrieves secret value from the given Secrets Manager store name.
     *
     * @param region            Region the secret is in
     * @param dbSecretStoreName Secrets Manager store name
     * @return JSON object inside the store
     */
    public static JSONObject getSecret(String region, String dbSecretStoreName) {

        System.out.println(String.format("Region: %s. dbsecrectstorename:%s", region, dbSecretStoreName));

        SecretsManagerClient secretsClient = SecretsManagerClient.builder()
                .region(Region.of(region))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();

        try {
            GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                    .secretId(dbSecretStoreName)
                    .build();

            GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
            String secret = valueResponse.secretString();
            JSONObject jo = new JSONObject(secret);

            return jo;

        } catch (SecretsManagerException e) {
            System.out.println(e.awsErrorDetails().errorMessage());
            throw new RuntimeException(e);

        } finally {
            secretsClient.close();
        }
    }
}