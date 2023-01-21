# Example-4

Example number 4 from Chapter 5 of [Bootstrapping Microservices](https://www.bootstrapping-microservices.com).

In Chapter 5 of Bootstrapping Microservices you learn how to make microservices talk to each other with HTTP requests and RabbitMQ messages.

You need Docker and Docker-Compose installed to run this.

Boot it up from the terminal using:

    docker-compose up --build

NOTE: followed by this instuction

> Modify the example-4 codes such that (after the command "docker-compose up --build") you could 
> - hit http://localhost:4001/video?id=1 in your browser, then you should see "a short video 1" (an mp4 file streaming > - may be the same mp4 file that we already have) on the browser, and the history microservice will insert a new entry (viewed-1) in the videos collection in the MongoDB, and the recommendation microservice prints out "recommendation video 1" to the console.
> - hit http://localhost:4001/video?id=2 in your browser, then you should see "a short video 2" (another mp4 file streaming) on the browser, and the history microservice will insert a new entry (viewed-2) in the videos collection in the MongoDB, and the recommendation microservice prints out "recommendation video 2" to the console.
