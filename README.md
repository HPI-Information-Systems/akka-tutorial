# Akka tutorial

The two projects akka-tutorial and octopus demonstrate how to use the actor programming framework [Akka](https://akka.io/) with Java in a distributed setting. The akka-tutorial project is a very simple grid computing app for determining all prime numbers in a given range of numbers. The octopus project, in contrast, performs a binary tree traversal where each node represents some arbitrary work that needs to be performed. Both projects let you start a master system that can be supported in its tasks by arbitrary many slave systems.
For a high-level overview of the apps' architectures, have a look at the [tutorial slides](https://docs.google.com/presentation/d/1acpitw8X9LoQbbFbua_Vl8zRmRWWAAiuJ4B8tYWx0zg/edit?usp=sharing) and the [teaching slides](https://hpi.de/naumann/teaching/teaching/ws-1819/distributed-data-management-vl-master.html).

## Requirements

In order to build and execute the code, you will need Java 8 and Maven.
To make sure that your project is set up correctly in an IDE, you can run the tests in the `akka-tutorial/src/test/java` folder. If you are operating from a command line instead, run `mvn test` in the folder with `pom.xml` file.

## Execution instructions

Just run the main class `de.hpi.akka_tutorial.Main` or `de.hpi.octopus.OctopusApp`, respectively, from within your IDE or from the command line. The app will then print an overview of the different possible parameters. Append parameters of your choice to the run configuration in your IDE or to your command line call, as exemplified below:
* Parameters to start a master with two local workers: `master --workers 2`
* Parameters to start a slave that tries to connect to a remote master: `slave --master <master host>:<master port>`

