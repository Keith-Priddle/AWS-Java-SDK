package com.mct.aws.ddb;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.amazonaws.regions.Regions;

import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

public class Application {
	
	private static final String AWS_REGION = "AWS_REGION";
	private static final String KEY = "Quote";
	
	public static final String PRIMARY_DB = "query";
	
	
    DynamoDbClient ddb;
    
    
	public Application(DynamoDbClient ddb) {
		this.ddb = ddb;
	}
	
	public static String createTable(DynamoDbClient ddb, String tableName, String key) {
		//String response = "Hello DynamoDB";
		DynamoDbWaiter dbWaiter = ddb.waiter();
		CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(key)
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .keySchema(KeySchemaElement.builder()
                        .attributeName(key)
                        .keyType(KeyType.HASH)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(new Long(10))
                        .writeCapacityUnits(new Long(10))
                        .build())
                .tableName(tableName)
                .build();
		
        String newTable ="";
		
        try {
            CreateTableResponse response = ddb.createTable(request);
            DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build();

            // Wait until the Amazon DynamoDB table is created
            WaiterResponse<DescribeTableResponse> waiterResponse =  dbWaiter.waitUntilTableExists(tableRequest);
            waiterResponse.matched().response().ifPresent(System.out::println);

            newTable = response.tableDescription().tableName();
            return newTable;

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
       return "";
		
	
	}
	
	public static void putItemInTable(DynamoDbClient ddb, 
			String tableName, String id, String idvalue, String quote, String quotevalue) {
		
		HashMap<String,AttributeValue> itemValues = new HashMap<String,AttributeValue>();

        // Add all content to the table
        itemValues.put(id, AttributeValue.builder().s(idvalue).build());
        itemValues.put(quote, AttributeValue.builder().s(quotevalue).build());

        PutItemRequest request = PutItemRequest.builder()
                .tableName(tableName)
                .item(itemValues)
                .build();

        try {
            ddb.putItem(request);
            System.out.println(tableName +" was successfully updated");

        } catch (ResourceNotFoundException e) {
            System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
            System.err.println("Be sure that it exists and that you've typed its name correctly!");
            System.exit(1);
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

	}
	
	
	public static String getItemsFromTable(DynamoDbClient ddb, String tableName, String key, String keyVal) {
		
		HashMap<String,AttributeValue> keyToGet = new HashMap<String,AttributeValue>();

        keyToGet.put(key, AttributeValue.builder()
                .s(keyVal).build());		
		
		GetItemRequest request = GetItemRequest.builder()
				.key(keyToGet)
				.tableName(tableName)
				.build();
		
		try {
			//GetItemResponse response = ddb.getItem(request);
			//System.out.println(response.toString());
			Map<String,AttributeValue> returnedItem = ddb.getItem(request).item();

            if (returnedItem != null) {
                Set<String> keys = returnedItem.keySet();
                System.out.println("Amazon DynamoDB table attributes: \n");

                for (String key1 : keys) {
                    System.out.format("%s: %s\n", key1, returnedItem.get(key1).toString());
                }
            } else {
                System.out.format("No item found with the key %s!\n", key);
            }
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		return "";
	}
	
	/*
	 * Update an existing item in the table
	 */
	public static void updateItem(DynamoDbClient ddb, String tableName, String key, String keyVal, String name, String updateVal) {
	
		HashMap<String, AttributeValue> keyToGet = new HashMap<String, AttributeValue>();
		
		keyToGet.put(key, AttributeValue.builder()
				.s(keyVal).build());
		
		HashMap<String, AttributeValueUpdate> updatedValues = new HashMap<String, AttributeValueUpdate>();
		updatedValues.put(name, AttributeValueUpdate.builder()
				.value(AttributeValue.builder().s(updateVal).build())
				.action(AttributeAction.PUT)
				.build());
		
		
		UpdateItemRequest request = UpdateItemRequest.builder()
				.tableName(tableName)
				.key(keyToGet)
				.attributeUpdates(updatedValues)
				.build();
		
		try {
			ddb.updateItem(request);
			
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		
		
	}
	 
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Region region = Region.EU_WEST_2;	
		DynamoDbClient ddb = DynamoDbClient.builder()
	            .region(region)
	            .build();
		
		//System.out.println(Application.createTable(ddb, PRIMARY_DB, KEY));
		//Application.putItemInTable(ddb, PRIMARY_DB,"id", "3", "quote", "Whoop...whooop...whoooop!!!");
		Application.getItemsFromTable(ddb, PRIMARY_DB, "id", "3");
		Application.updateItem(ddb, PRIMARY_DB, "id", "2", "quote", "Ho.. Ho.. Ho...");
	}

	
}
