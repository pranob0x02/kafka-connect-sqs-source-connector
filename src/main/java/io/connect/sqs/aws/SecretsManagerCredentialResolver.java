package io.connect.sqs.aws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

/**
 * Resolves AWS credentials from AWS Secrets Manager.
 * <p>
 * The secret must be a JSON object containing keys for access key ID and secret access key.
 * Default key names are "aws_access_key_id" and "aws_secret_access_key", but can be configured.
 * <p>
 * Example secret JSON:
 * <pre>
 * {
 *   "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
 *   "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
 * }
 * </pre>
 */
public class SecretsManagerCredentialResolver {

    private static final Logger log = LoggerFactory.getLogger(SecretsManagerCredentialResolver.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String region;

    public SecretsManagerCredentialResolver(String region) {
        this.region = region;
    }

    /**
     * Resolves credentials from a Secrets Manager secret.
     *
     * @param secretArn ARN or name of the secret
     * @param accessKeyField JSON field name for the access key ID
     * @param secretKeyField JSON field name for the secret access key
     * @return StaticCredentialsProvider with the resolved credentials
     */
    public AwsCredentialsProvider resolve(String secretArn, String accessKeyField, String secretKeyField) {
        log.info("Resolving credentials from Secrets Manager: {}", secretArn);

        try (SecretsManagerClient client = SecretsManagerClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            GetSecretValueResponse response = client.getSecretValue(
                    GetSecretValueRequest.builder()
                            .secretId(secretArn)
                            .build());

            String secretString = response.secretString();
            if (secretString == null || secretString.isEmpty()) {
                throw new IllegalStateException("Secret value is empty for: " + secretArn);
            }

            JsonNode secretJson = MAPPER.readTree(secretString);

            JsonNode accessKeyNode = secretJson.get(accessKeyField);
            JsonNode secretKeyNode = secretJson.get(secretKeyField);

            if (accessKeyNode == null || accessKeyNode.isNull()) {
                throw new IllegalStateException(
                        "Secret '" + secretArn + "' does not contain field: " + accessKeyField);
            }
            if (secretKeyNode == null || secretKeyNode.isNull()) {
                throw new IllegalStateException(
                        "Secret '" + secretArn + "' does not contain field: " + secretKeyField);
            }

            String accessKey = accessKeyNode.asText();
            String secretKey = secretKeyNode.asText();

            log.info("Successfully resolved credentials from Secrets Manager (access key: {}...)",
                    accessKey.substring(0, Math.min(4, accessKey.length())));

            return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey));

        } catch (Exception e) {
            throw new RuntimeException("Failed to resolve credentials from Secrets Manager: " + secretArn, e);
        }
    }

    /**
     * Retrieves a single field value from a Secrets Manager secret.
     *
     * @param secretArn ARN or name of the secret
     * @param fieldName JSON field name to retrieve
     * @return the field value as a string
     */
    public String resolveField(String secretArn, String fieldName) {
        log.debug("Resolving field '{}' from Secrets Manager: {}", fieldName, secretArn);

        try (SecretsManagerClient client = SecretsManagerClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            GetSecretValueResponse response = client.getSecretValue(
                    GetSecretValueRequest.builder()
                            .secretId(secretArn)
                            .build());

            String secretString = response.secretString();
            if (secretString == null || secretString.isEmpty()) {
                throw new IllegalStateException("Secret value is empty for: " + secretArn);
            }

            JsonNode secretJson = MAPPER.readTree(secretString);
            JsonNode fieldNode = secretJson.get(fieldName);

            if (fieldNode == null || fieldNode.isNull()) {
                throw new IllegalStateException(
                        "Secret '" + secretArn + "' does not contain field: " + fieldName);
            }

            return fieldNode.asText();

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve field '" + fieldName + "' from Secrets Manager: " + secretArn, e);
        }
    }
}
