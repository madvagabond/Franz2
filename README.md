# Franz2
A new and improved version of Franz using Finagle. The aformentioned improvements include deliveree gaurentees, cluster support, an asynchronous client, and it is written in a more functional style. 

Tutorial 
//start server 

val addr = "127.0.0.1:3000"

utils.makeServer(addr)

//create publish and subscribe. 

def f(n: Int)(x: String) = println(s" $x #$n")

client.createTopic(addr, "boing")

(1 to 10) foreach { x => client.subscribe(addr, "boing")(f(x)) }

(1 to 1000).foreach {_ => client.publish(addr, "boing", "gratatata")}

//make cluster

val members = ("127.0.0.1:3000", "127.0.0.1:4000", "127.0.0.1:5000")

members.foreach{y => utils.clusteredFranz(y, members)}
