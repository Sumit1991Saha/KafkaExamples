To generate the model file from avro file (.avsc) :-
java -jar /Users/sumit.saha/Desktop/ApacheTools/avro-tools-1.8.2.jar compile schema /Users/sumit.saha/codebase/KafkaExamples/src/main/java/com/saha/schemas/User.avsc /Users/sumit.saha/codebase/KafkaExamples/src/main/java/
 
Create following topics :-
simple-topic
order-topic
order-topic-filtered
valid-order-count-topic

 
Following examples are covered in this :-
1. Simple Producer / Consumer API's, 
   To run it go to src/main/java/com/saha/producerconsumer/simple
   and run both the producer and consumer code.
2. Using Avro to send/receive messages, 
   To run it go to src/main/java/com/saha/producerconsumer/avro
   and run both the producer and consumer code.
   (generate the avro (user.avsc) model first by running the above mentioned command on line no. 1)
3. Kafka Streams app to consume the message using Kafka DSL :-
   To run it go to src/main/java/com/saha/producerconsumer/ and run the producer, 
   then go to dsl folder and run DSLApp
   (generate the avro (order_schema.avsc) model first by running the above mentioned command on line no. 1)
4. Kafka Streams app to consume the message using Kafka processor api :-
   To run it go to src/main/java/com/saha/producerconsumer/ and run the producer, 
   then go to processor folder and run ProcessorApp
   (generate the avro (order_schema.avsc) model first by running the above mentioned command on line no. 1)