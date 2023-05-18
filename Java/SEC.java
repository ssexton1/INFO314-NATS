import java.io.*;
import java.sql.Timestamp;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Dispatcher;
import io.nats.client.Message;

public class SEC {
  private static String STARTPATH = System.getProperty("user.dir");
  private static String BROKER = "";

  public static void main(String... args) {
    String natsURL = "nats://127.0.0.1:4222";
    // if (args.length > 0) {
    //   natsURL = args[0];
    // }

    try {
      Connection nc = Nats.connect(natsURL);
      Dispatcher market = nc.createDispatcher((msg) -> {
        try {
          System.out.printf("%s on subject %s\n",
            new String(msg.getData()),
            msg.getSubject());
          processMessage(msg);
        } catch (Exception e) {
          e.printStackTrace();
        }

      });

      market.subscribe(">");

    } catch (Exception e) {
      e.printStackTrace();
    }

  }


  private static void processMessage(Message msg) throws Exception {
    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc = builder.parse(new ByteArrayInputStream(msg.getData()));
    XPath xPath = XPathFactory.newInstance().newXPath();

    boolean isRequest = (boolean) xPath.compile("/order").evaluate(
      doc, XPathConstants.BOOLEAN);

    boolean isReply = (boolean) xPath.compile("/orderReceipt").evaluate(
      doc, XPathConstants.BOOLEAN);

    if (isRequest) {
      processRequest(msg);
    } else if (isReply) {
      processReply(msg);
    }
  }


  private static void processRequest(Message msg) throws Exception {
    String subject = new String(msg.getSubject());
    String[] subjectArr = subject.split("[.]");
    BROKER = subjectArr[1];
  }


  private static void processReply(Message msg) throws Exception {
    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc = builder.parse(new ByteArrayInputStream(msg.getData()));
    XPath xPath = XPathFactory.newInstance().newXPath();

    String totalCost = (String) xPath.compile("/orderReceipt/complete/@amount").evaluate(
      doc, XPathConstants.STRING);


    if (Integer.parseInt(totalCost) > 500000) {
      Node orderReceipt = (Node) xPath.compile("/orderReceipt").evaluate(
        doc, XPathConstants.NODE);

      Node orderSent = orderReceipt.getFirstChild();

      String client = new String(msg.getSubject());

      Timestamp timestamp = new Timestamp(System.currentTimeMillis());

      buildLog(doc, timestamp, client, orderSent, totalCost);
    }

  }


  private static void buildLog(Document doc, Timestamp time, String client, Node orderSent, String totalCost) throws Exception {
    StringBuilder sb = new StringBuilder();
    XPath xPath = XPathFactory.newInstance().newXPath();

    String orderType = orderSent.getNodeName();

    String symbol = (String) xPath.compile("/orderReceipt/" + orderType + "/@symbol").evaluate(
      doc, XPathConstants.STRING);
  
    String amount = (String) xPath.compile("/orderReceipt/" + orderType + "/@amount").evaluate(
      doc, XPathConstants.STRING);

    sb.append("Timestamp: " + time + ", ");
    sb.append("Client: " + client + ", ");
    sb.append("Broker: " + BROKER + ", ");
    sb.append("Order Sent: <" + orderType + " symbol=\"" + symbol + "\" amount=\"" + amount + "\" />");
    sb.append("Amount: " + totalCost + "\n");

    logSuspicions(sb.toString());
  }


  private static void logSuspicions(String log) throws Exception {
    System.out.println(STARTPATH);
    File newFile = new File(STARTPATH + "/suspicions.log");
    newFile.createNewFile();

    FileOutputStream fos = new FileOutputStream(newFile, true);
    fos.write(log.getBytes());
  }
}
