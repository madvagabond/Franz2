# Franz2
A new and improved version of Franz using Finagle. The aformentioned improvements include deliveree gaurentees, cluster support, an asynchronous client, and it is written in a more functional style. 

Tutorial 
//installation 

//requires sbt 

git clone https://github.com/XnuKernPoll/Franz2

cd ./franz2 

sbt publish-local 

// add dependency 
in your build.sbt 

libraryDependencies += "com.franz" % "franz2_2.10" % "0.1-SNAPSHOT"

//start server 
import com.franz.franz2._

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
