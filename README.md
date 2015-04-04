# TwitterProcessing

This is a project to demonstrait some simple items about dealing with Twitter, Accumulo, and MapReduce.

In order to work with data, you first need the data.  This is espetially true for dealing with Big Data.
Twitter provides what they call their Garden Hose.  This is a fraction of what they call their Fire Hose.
The Fire Hose is all tweets.  I don't know the actual fraction of the whole is in the Garden Hose.
Whatever the case is, when connecting to the Garden Hose, you can certainly pull in a suitable large
number of items that will allow you to work with Hadoop, Spark, Storm, or whatever.  Hopefully, you
will find this small project useful for learning an possible some larger project.

Getting things to work with Twitter:
Before you can start connecting to twitter to get data, you first must register as a developer.  Once
that is done, you must register your application.  Here are the steps you need to go through in order to
get things going.
1) Create a Twitter account.  Go to https://twitter.com/
2) Now register an application with Twitter.  Go to https://apps.twitter.com/app/new to register.
3) Make sure you keep the keys you are given.  Put the values into a configuration file of the format:

twitterKey = <your key>
twitterSecret = <your secret>

4) Now build the project.
mvn clean install

5) Now build the jar to run programs.
mvn package -P ingest

6) Now run the BasicTwitterConnection appleication while using the keys you downloaded.  This will retrieve a set of other keys.  These will be the Token key and Token secret.
java -cp TwitterProcessing-0.0.1-SNAPSHOT-phat.jar org.ptp.twitter.ingest.GetTwitterAppKeys -c configFile

With the results, add to your configuration file with the following format:

twitterKey = <your key>
twitterSecret = <your secret>
twitterAccessToken = <access token>
twitterAccessTokenSecret = <access secret>

You can view the Twitter api documentation here: https://dev.twitter.com/overview/documentation

7) Now, with your keys, try the connection to pull data:
java -cp TwitterProcessing-0.0.1-SNAPSHOT-phat.jar org.ptp.twitter.ingest.TwitterConnect -c configFile -o outputDir

