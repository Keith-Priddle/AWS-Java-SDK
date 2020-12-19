package com.mct.aws.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;




public class S3BucketCreate {

	private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
	private static S3Client s3;
	//private static S3Waiter s3Waiter;
	//create an object of SingleObject
	//private static S3BucketCreate instance = new S3BucketCreate(s3);

	   
	
	
	public S3BucketCreate(S3Client s3) {
		this.s3 = s3;
		//this.s3Waiter = s3Waiter;
	}
	
	
	
	public void createBucket(String name) {
		try {
			S3Waiter s3Waiter = s3.waiter();
			CreateBucketRequest request = CreateBucketRequest
					.builder()
					.bucket(name)
					.build();
			s3.createBucket(request);
			
			HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
	                    .bucket(name)
	                    .build();

	            // Wait until the bucket is created and print out the response
	            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
	            waiterResponse.matched().response().ifPresent(System.out::println);
	            System.out.println(name +" is ready");

			
			
			
		} catch(Exception e) {
		LOGGER.error("error during create bucket", e);
		}
	}
}
