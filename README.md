
Messaging service with RabbitMQ

RabbitMQ queue has been applied to this Java code that sends requests to a messaging service API. The SendSMSPublisher code is responsible for building requests (that are to be sent to the messaging service API) that are sent to a RabbitMQ queue and are processed by the SendSMSConsumer code. The SendSMSConsumer tries to send these requests to the API in the required format and checks if the request has been processed successfully. If even after many tries the request is not successfully sent, those messages are stored in the Dead Letter Queue.
