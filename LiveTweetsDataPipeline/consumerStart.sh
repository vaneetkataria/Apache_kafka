#This Script can build and start producer as specified.
#Init an action variable with command line argument . 
action=$1
###Build the project again only if -b or nothing is specified.
if [ "$action" = "-b" ] ; then 
 #change directory to To TweetsProducer 
 cd TweetsConsumer
 mvn clean install
 #check if maven build was a failure then exit the script . 
 if [ $? != 0 ]; then
  echo "#####Maven build failed exiting."
  exit $rc
 fi
 #return back to script directory.
 cd ..
fi
#start producer
#Navigating to target directory of TweetsProducer
java -jar  TweetsConsumer/target/TweetsConsumer-0.0.1-SNAPSHOT.one-jar.jar 




