package com.urvin.sqs.ElasticMQ;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import java.util.List;

public class SQSQueue {
    private AmazonSQSClient amazonSQSClient;
    private String queueUrl;

    public SQSQueue(String queueName) {
        this.amazonSQSClient = new AmazonSQSClient(new ProfileCredentialsProvider());
        this.amazonSQSClient.configureRegion(Regions.AP_NORTHEAST_1);
        this.amazonSQSClient.withTimeOffset(10);
        // Create Queue
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
        this.queueUrl = amazonSQSClient.createQueue(createQueueRequest).getQueueUrl();
    }

    private void sendMessage(String message) {
        this.amazonSQSClient.sendMessage(queueUrl,message);
    }

    private List<Message> receiveMessages(int maxMessages) {
        ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl);
        request.setMaxNumberOfMessages(maxMessages);
        return this.amazonSQSClient.receiveMessage(request).getMessages();
    }

    public static void main(String[] args) {
        SQSQueue queue = new SQSQueue("TextMesage");
        queue.sendMessage("Hello World");
        queue.sendMessage("Hello World1");
        queue.sendMessage("Hello World2");
        queue.sendMessage("Hello World3");
        queue.sendMessage("Hello World4");
        queue.sendMessage("Hello World5");

        List<Message> messages = queue.receiveMessages(6);
        if(messages != null) {
            messages.forEach(message -> {
                System.out.println("Message body is :"+message.getBody());
            });
        }
    }

}
