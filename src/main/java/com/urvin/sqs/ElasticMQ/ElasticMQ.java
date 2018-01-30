package com.urvin.sqs.ElasticMQ;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

import java.util.List;

public class ElasticMQ {

    private AmazonSQSClient amazonSQSClient;
    private CreateQueueResult queueResult;
    private String queueUrl;
    private AmazonSQS sqs;

    public ElasticMQ(String queueName) {
        amazonSQSClient = new AmazonSQSClient(new BasicAWSCredentials("x","x"));
        String endPoint = "http://localhost:9324";
        amazonSQSClient.setEndpoint(endPoint);
        // Set queueUrl
        this.queueUrl = endPoint+"/queue/"+queueName;

        // Create Queue
        queueResult = amazonSQSClient.createQueue(queueName);
    }

    public static void main(String[] args) {
        ElasticMQ queue = new ElasticMQ("TestMessage");
        queue.sendMessage("Hello World");
        queue.sendMessage("Hello World1");
        queue.sendMessage("Hello World2");
        List<Message> messages = queue.receiveMessages(3);

        if(messages != null) {
            System.out.println("Total message recieved :"+messages.size());
            messages.forEach(message -> {
                System.out.println("Message Body is:"+message.getBody());
            });
        }
    }

    private void sendMessage(String message) {
        amazonSQSClient.sendMessage(new SendMessageRequest(queueUrl,message));
    }

    private List<Message> receiveMessages(int maxMessages) {
        ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl);
        request.setMaxNumberOfMessages(maxMessages);
        return amazonSQSClient.receiveMessage(request).getMessages();
    }
}
