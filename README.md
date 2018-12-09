# Kafka Streams and KTables examples

This code is the companion of the blog post [Kafka ](http://danlebrero.com/)

This project uses Docker to create a cluster of 3 microservices that consume from a Kafka topic using the
Kafka Streams API.

The main processing function is [here](our-service/src/our_service/big_file.clj#L115).

## Usage

Docker should be installed.

To run:

     docker-compose up -d --build && docker-compose logs -f our-service our-service2 our-service3
     
Once the environment has been started, you have to add generate some files with:

     curl --data "lines=100&size=200" -X POST http://localhost:3004/big-file

## Clean up

To get rid of all:

    docker-compose down --rmi all --remove-orphans
    docker image rm pandeiro/lein:2.5.2 wurstmeister/kafka:2.11-2.0.0
    
