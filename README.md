# Akka tutorial

This repository contains some tutorial and exercise projects for the actor programming toolkit [Akka](https://akka.io/). It is maintained by the [information systems group](https://hpi.de/naumann/home.html) at [HPI](https://hpi.de/) and used in distributed computing classes. The projects are implemented in Java 8 and run distributedly on a computer cluster.

# Projects

The akka-tutorial project is a very simple grid computing app for determining all prime numbers in a given range of numbers. The implementation is very basic and uses only the core concepts of the Akka toolkit, i.e., simple remoting with additional libraries, such as Akka clustering. 

The octopus project showcases a binary tree traversal where each node represents some arbitrary work that needs to be performed. It uses Akka's clustering module.

The ddm project is a framework project for exercises in the lecture [Distributed Data Management](https://hpi.de/naumann/teaching/teaching/ws-1920/distributed-data-management-vl-master.html). It also uses Akka's clustering module for the distributed computing.

All projects can be startet as master or slave systems. The slave systems connect to the master and support its calculations. There should be only one master but arbitrary many slave systems.
For a high-level overview of the apps' architectures, have a look at the [tutorial slides](https://docs.google.com/presentation/d/1acpitw8X9LoQbbFbua_Vl8zRmRWWAAiuJ4B8tYWx0zg/edit?usp=sharing), the [teaching slides 2018](https://hpi.de/naumann/teaching/teaching/ws-1819/distributed-data-management-vl-master.html) and the [teaching slides 2019](https://hpi.de/naumann/teaching/teaching/ws-1920/distributed-data-management-vl-master.html).

## Requirements

To build and execute the code, Java 8 and Maven are required.
To make sure that the projects are set up correctly in an IDE, you can run the tests in the `akka-tutorial/src/test/java` folder. If you are operating from a command line instead, run `mvn test` in the folder with `pom.xml` file.

## Execution instructions

The projects can be started by running their main classes, which are `de.hpi.akka_tutorial.Main`, `de.hpi.octopus.OctopusApp`, and `de.hpi.ddm.Main`, respectively, from within your IDE or from the command line. The app will then print an overview of the different possible parameters. Append parameters of your choice to the run a certain configuration. For example, calls could be as follows:
* Parameters to start a master with two local workers: `master --workers 2`
* Parameters to start a slave that tries to connect to a remote master: `slave --master <master host>:<master port>`

