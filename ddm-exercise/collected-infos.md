# Infos about each file / class we collected via reading the source

## Worker System
- Creates an ActorSystem with the given configuration, the host ip + port and as worker role.
- Creates a reaper
- After spinning up the Actor System it uses the cluster once the clusters changed from "joining" to "up" to start the given number of worker actors
- If the status of the cluster is changed to "Removed" then the actor system is terminated gracefully.

## Master System
- Creates an ActorSystem with the given configuration, the host ip + port and as master role.
- It creastes a reaper actor, reader actor, collector actor, master actor
- After spinning up the Actor System it uses the cluster once the clusters changed from "joining" to "up" to start the given number of worker actors. Then it sends a start message to the master actor when the config singleton does not say that the system is paused on start.
- If the status of the cluster is changed to "Removed" then the actor system is terminated gracefully.
- Lastly, if the config singleton says to not start immediatly, the system waits for the user to press enter in the console and then sends the start message to the master.

## BloomFilter
We got a bloom filter implementation that I do not understand. Afaik it makes the check whether an element is in a set pretty fast and therefore is useful.
But it does not take ints as an input but BitSets whose hashcodes are used?! Thats strange...



