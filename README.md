# INFO314-NATS

Welcome! This is a group project homework for INFO 314. Begin by forking this repository into the GitHub account your group is using for this assignment. (It is strongly encouraged that you each individually also create forks of the repository into your own accounts at the end of this assignment--interviewers will want to see this!)

You are going to be experimenting with a "messaging system" called [NATS](https://nats.io/) (also long ago known as "gnatsd"). It has copious amounts of documentation on its website, including support for all mainstream programming languages (and quite a few that aren't considered mainstream). In this homework, we will be using NATS only for its most basic of functionality, but you are encouraged to experiment and explore it further, as it is quite versatile and quite powerful.

> **NOTE:** There are a number of other messaging systems out there--ActiveMQ, RabbitMQ, and so on--and Java even has a standarized API for working with them ("Java Messaging Service" or JMS). NATS, however, is easy to install and run, and so was the preferred choice for a homework exercise. If, for some reason, your group has a strong desire to run a different messaging system, please email myself and the TA immediately, so that we can evaluate whether (1) it is a reasonable substitute, and (2) it will not create more problems than solutions to use.

## Goals

In this particular assignment, you are going to be building out a system of "stock market" principals: a "publisher" that will mimic the ups and downs of a given stock market (remember, there are many different stock markets), a "monitor" that will watch the ups and downs and report on what it sees, and a "broker" and "clients" that will mimic the activities of a stock broker (buying and selling particular stocks) and their clients (who have particular strategies for when to buy and when to sell). Messages will be XML data, in order to maximize interoperability across languages/platforms.

## Steps

Ensure that you have a NATS server installed locally; it is a single binary, which you can either obtain from visiting the NATS website's [download](https://nats.io/download/) page, or use your platform's package manager to obtain it, or you can use the two binaries stored in this repository (if you are running macOS or Windows).

> **NOTE:** If you want to keep your laptop "clean", it's often advisable to run a new technology (such as NATS) in a Docker container rather than on the "bare metal" of your laptop. In the case of NATS, you can do this via the command-line `docker run -d --name nats-main -p 4222:4222 -p 6222:6222 -p 8222:8222 nats`, assuming you have Docker installed. This will download the official NATS image to your machine, open ports 4222, 6222, and 8222 and bind them to the same port numbers in the container, then run NATS as a server inside that container. Assuming your machine has enough CPU and memory to run a Docker container (or two or three), then it's a highly-advisable way to explore new things.

In a Terminal/console window, navigate to the directory the server is stored, and start it by typing `nats-server`. This will start the server, listening for connections on port 4222 (it's default port). 

> **NOTE:** Running NATS on its default port is sufficient for this homework, but if you wish you can run it with some additional command-line parameters, such as `--http_port` to specify a port on which you can get information about the running server in your web browser. Run `nats-server --help` to get a complete list of options.

At this point, the messaging server is up and running, and you shouldn't need to do anything further to it. Take note of the port the server is running on (4222 if you didn't spcify otherwise; look for the log message `[INF] Listening for client connections on 0.0.0.0:4222` in the console window running the server if you're not sure), as that will be part of the "NATS URL" you will need to pass to various clients in order to connect to your running NATS server.

> **NOTE:** A "NATS URL" looks very similar to an HTTP URL, but uses the "nats" scheme instead of "http"; thus, for most situations, connecting to a NATS server running on your local machine will require a URL of `nats://localhost:4222` or `"nats://127.0.0.1:4222"`.

Now you need to start building out the pieces that will make up your system. Your goal, over time, is to have all of these pieces running simultaneously, on different machines, with video evidence proving it.

## Stories/Rubric: 20 pts

Each of these components can either be a standalone program (recommended) with its own `main` entrypoint, so that you can run as many or as few simultaneously as you wish. (It's also one of the major strengths of a messaging-based system--if you need more, just run more.)

For all of these components, you must be able to pass a NATS server URL on the command-line to specify where the NATS server is running.

### StockPublisher (5 pts)
The first component you will need to build is the StockPublisher. This component will connect to the NATS server and spin up three "stock markets": a StockMarketRunner that will create a set of stock symbols (the alphabetic short-codes for companies on a given stock market, like "MSFT" for Microsoft of "AMZN" for Amazon) and their starting prices, and then enter an infinite loop. While in that loop, it will wait for a short period of time. When it wakes up, it will pick a stock symbol at random, create a price "adjustment" anywhere from -$5 to +$5, and adjust the current price of that stock to that amount.

Your job will be spin up at least three stock markets (each represented by a StockMarketRunner) with roughly seven to ten stock symbols in each. You are free to have more, but you must have *at a minimum* three markets each with 7-10 stocks apiece. (Make sure the symbols are unique--each company can only be in one stock market inside your group.) Each of these StockMarketRunners must be started up inside their own Thread.

The StockPublisher will connect to the NATS server, and the static `publish` method on that class will do the work of publishing to NATS. Note that the method is marked `synchronized`: this is a simple way to ensure that your threads do not clobber each other while using it to publish messages.

Speaking of which, the messages your publisher sends out should use XML as the wire format and look like the following:

```
<message sent="(timestamp)">
  <stock>
    <name>(symbol)</name>
    <adjustment>(amount)</adjustment>
    <adjustedPrice>(price after adjustment)</adjustedPrice>
  </stock>
</message>
```

You are free to add additional XML elements to these messages if you wish, but the above must remain consistent in the face of any additions.

### StockMonitor (5 pts)

The StockMonitor is a message consumer that will listen to messages sent to the NATS server, and specifically it will either listen to all symbols published to the server, or it will listen for only those stock symbols passed on the command-line. That is, if the StockMonitor is run with command-line parameters of "MSFT", "AMZN" and "BLIZ", it will only listen for adjustments to those three stocks. Any stock symbols can be specified, regardless of which market they are in.

The StockMonitor must create "price log" files, one for each symbol it is tracking. A "price log" file is a text file, with each line consisting of the timestamp a message is received, the adjustment for that message, and the current price of the stock after adjustment. (In other words, the StockMonitor needs to log each message in a symbol-specific file.)

When you run StockMonitors in your video(s) (below), make sure to have one that watches all symbols, as well as a few that each watch some symbols (but not all).

### StockBroker (3 pts)

StockBrokers are the components that will work with StockBrokerClients, where each client will have a portfolio of stocks and a "strategy" as to when to buy or sell particular stocks. StockBrokers are uniquely named (give each StockBroker a `name` constructor parameter that is used to identify this StockBroker everywhere in the system), and clients choose which StockBroker they use. When the client wishes, they will send "buy" messages that look like the following:

```
<order><buy symbol="(name)" amount="(number of shares)" /></order>
```

... and a "sell" message will look similarly, except with a tag name of `sell` replacing `buy`.

The StockBroker must receive the message from one of their clients (and not intercept any client messages to other brokers!) and carry out the request. (In this particular implementation, "carry out the request" means "sleep for a few seconds".) Once the request is carried out, it must send a reponse to the client with the total amount in the order (either the cost of the buy, or the amount of the sell); in the response, the Broker must charge 10% of the total transaction, so a "buy" order response will need to include the 10% the Broker charges as its fee, and the "sell" order response will need to subtract the 10% the Broker charges as its fee. The responses look like the following:

```
<orderReceipt><(original order here)><complete amount="(amount after fee)" /></orderReceipt>
```

Thus, if the client sent an order of `<order><sell symbol="MSFT" amount="40" /></order>` and the current price at the time for MSFT stock is 5000 ($50), then the Broker sends back `<orderReceipt><sell symbol="MSFT" amount="40" /><complete amount="180000" /></orderReceipt>` (5000 * 40 = 200000, minus 200000 / 10 = 20000).

### StockBrokerClient (4 pts)

Lastly the StockBrokerClient is the other side of the broker/client relationship. Each client, who works exclusively with one StockBroker (the Client knows the name of the Broker they work with--you can either pass this as a literal string into the constructor or you can have the StockBrokerClient take it as a command-line parameter, your choice), and has a "portfolio" of stock (basically a pairing of symbol and integer numbers of shares) and a "strategy" describing when they want to buy certain stocks or sell certain stocks.

A client's portfolio file looks like the following:

```
<portfolio>
  <stock symbol="(symbol)">(number of shares)</stock>
</portfolio>
```

and their strategy file looks like this:

```
<strategy>
  <!-- This strategy says, when (symbol) is above (share price), sell all the shares we have -->
  <when><stock>(symbol)</stock><above>(share price)</above><sell /></when>
  <!-- This strategy says, when any stock is below 1000 ($10), buy 100 shares -->
  <when><stock /><below>1000</above><buy>100</buy></when>
  <!-- This strategy says, when any stock is above 10000 ($10), sell all of it -->
  <when><stock /><above>10000</above><sell /></when>
</strategy>
```

Each StockBrokerClient should be initialized with a "strategy" and "portfolio" parameter, indicating which portfolio and strategy files to read. You are free to create your own versions of these; in fact, it is encouraged, to go along with the stock symbols you created for your StockPublisher. Each StockBrokerClient is also initialized with a StockBroker name (to go along with the name of a StockBroker instance), to whom buy and sell orders will be sent.

Your StockBrokerClient must send messages when the StockPublisher shows a stock has reached some kind of threshold (either a high or a low, according to that client's strategy), and wait to receive a response from that broker when the order has been executed. Each StockBrokerClient must keep track of its portfolio, and if the StockBrokerClient receives an acknowledgement that its order has been carried out, update the client's "portfolio.xml" file appropriately.

The "Clients" directory under the root of this repository has several strategy and client files; don't use those directly, but copy them over each time you run. (That way you have some consistency in your testing.) 

> **NOTE:** Some of these rules could generate a LOT of traffic--if, for example, a client strategy has a rule of buying when the price is below $15 and selling when the price is above $16, and the price fluctuates from $14 to $18 and back again, there's a lot of buying and selling going on. You are free to modify the strategy files you are using, just (1) make sure they generate *some* orders, and (2) make sure they're checked in to your repository.

Keep in mind the clients have infinite money, but not infinite stock--if a client wants to sell stock they don't currently own, no order goes out. (For this reason, "buy" orders should always have an amount associated with them; it's possible to sell all of a client's stock, but not buy all of a company's stock!)

> **NOTE:** Anybody pointing out that Elon Musk did exactly that--buy out all of Twitter's stock--will be immediately put into a homework assignment that is WAY more complicated than this one. (Not really. But don't point it out--there's a lot of shortcuts we're taking here, in the interests of pedagogical sanity.)

### SEC (3 pts)

Meanwhile, the SEC (Securities and Exchange Commission) is always on the lookout for suspicious transactions, and transactions that are over $5000 are *always* suspicious. (Not really.) Write a `SEC` class that is able to see *all* of the client-broker orders, and write a line out to a file called "suspicions.log" that tracks the timestamp of the order, the client, the broker, the order sent, and the amount. (You don't need to stop the order, just log it--if the offline analysis determines there was any funny activity, the dedicated agents of the FBI will be happy to stop by either the client or the broker and have a chat.)

> **NOTE:** Anyone who suggests that in real life the SEC can't, or shouldn't, be able to monitor all the transactions will immediately be put on Elizabeth Warren's campaign donation mailing list. Repeatedly. With notes that say, "Please, Senator Warren, please explain SEC finance law to me ***in great detail***." You have been warned....

### Prove it!

For all of this, create a video or separate videos demonstrating that your implementations can achieve each of the following scenarios:

* one StockPublisher or multiple StockPublishers
* one StockMonitor or multiple StockMonitors
* one StockBroker with multiple StockBrokerClients using that one broker
* multiple StickBrokers each having multiple StockBrokerClients
* one SEC to keep a wary eye on everything (*everything*)

Alternatively, if you are camera shy or find it difficult to record a video for some reason, you may schedule time with the TA to show them your running code. However, this is *at the TA's discretion* and is in no way an obligation on their part--the video is the far more scalable option from our perspective.

**Video Link:** https://drive.google.com/drive/folders/1QyAo5QSkLKFrJ5Uy_gJTBZ9ZvElnfChy?usp=sharing 

## Extra Credit

### Secure it! (3 pts)

Run NATS so that all communication is done over TLS and requires all message publishers and consumers to authenticate against the server.

### Format it! (3 pts)

Instead of using XML, pass all messages around using JSON instead. In your video, show your code sending XML messages and then JSON messages, and offer up your group's opinion as to which you prefer. Are there situations where one is better? 

### Interop it! (4 pts)

Work with another group. Have both of your StockMarketPublishers run against a single NATS server and see how your systems react. If your StockMarketPublishers each have the same symbol (such as each group is publishing "MSFT" changes), figure out ways to continue to work *without changing the stock symbol on either side*. (In other words, group A's StockBrokerClients with MSFT in their portfolio should only be seeing and reacting to group A's MSFT price changes even if group B also has a MSFT symbol.)

By the way, make sure that *either* group A's `SEC` *or* group B's `SEC` can see all the activity going on, regardless of which NATS server it is happening on--the SEC knows (and needs to know!) everything!
