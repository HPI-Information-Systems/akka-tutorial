# Akka tutorial

This project demonstrates how to use the actor programming framework [Akka](https://akka.io/) with Java. It contains a simple grid computing app for determining all prime numbers in a given range of numbers.
For a high-level overview of the app's architecture, have a look at our [slides](https://docs.google.com/presentation/d/1acpitw8X9LoQbbFbua_Vl8zRmRWWAAiuJ4B8tYWx0zg/edit?usp=sharing).

## Requirements

In order to build and execute the code, you will need Java 8 and Maven.
To make sure that your project is set up correctly in an IDE, you can run the tests in the `src/test/java` folder. If you are operating from a command line instead, run `mvn test` in the folder with `pom.xml` file.

## Execution instructions

Just run the main class `de.hpi.akka_tutorial.Main` from within your IDE or from the command line. The app will then only print an overview of the different possible parameters. Simply append parameters of your choice to the run configuration in your IDE or to your command line call, as exemplified below:
* Parameters to start a master with two local workers: `master --workers 2`
* Parameters to start a slave that tries to connect to a remote master: `slave --master <master host>:<master port>`
