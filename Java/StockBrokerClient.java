import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Subscription;

/**
 * Usage: java StockBrokerClient [nats_url] [path_to_strategy.xml] [portfolio.xml]
 */
public class StockBrokerClient {
    private static final String PRICE_ADJUSTMENT_MSG_NAME = "PriceAdjustment";
    private static final String ORDER_SUBJECT = "Order";

    public static void main(String[] args) {
        String natsURL = (args.length > 0 && args[0] != "") ? args[0] : "nats://127.0.0.1:4222";
        String strategyPath = (args.length > 1 && args[1] != "") ? args[1] : "/strategy-1.xml";
        String portfolioPath = (args.length > 1 && args[1] != "") ? args[1] : "/portfolio-1.xml";

        try {
            Portfolio portfolio = new Portfolio(portfolioPath);
            Strategy strategy = new Strategy(strategyPath, portfolio);
            System.out.println(strategy.toString());

            Connection nc = Nats.connect(natsURL);

            Dispatcher priceAdjustSubDispatcher = nc.createDispatcher((msg) -> {
                try {
                    handlePriceAdjust(msg, strategy);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            });
            priceAdjustSubDispatcher.subscribe(PRICE_ADJUSTMENT_MSG_NAME);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void handlePriceAdjust(Message msg, Strategy strategy) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputStream msgStream = new ByteArrayInputStream(msg.getData());
        Document msgDoc = docBuilder.parse(msgStream);

        NodeList stocks = msgDoc.getElementsByTagName("stock");
        StockPrice[] stockPrices = new StockPrice[stocks.getLength()];
        for (int i = 0; i < stocks.getLength(); i++) {
            Element stock = (Element) stocks.item(i);

            String symbol = stock.getElementsByTagName("name")
                .item(0)
                .getTextContent();
            String adjustedPriceCents = stock.getElementsByTagName("name")
                .item(0)
                .getTextContent();

            stockPrices[i] = new StockPrice(symbol, Integer.parseInt(adjustedPriceCents));
        }
    private static class Portfolio {
        private Document document;
        private String path;
    
        public Portfolio(String portfolioPath) throws ParserConfigurationException, IOException, SAXException {
            DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            path = portfolioPath;
            document = docBuilder.parse(new File(path));
        }
    
        public int getShares(String symbol) {
            Element portfolioElement = document.getDocumentElement();
            NodeList stockNodes = portfolioElement.getElementsByTagName("stock");
    
            for (int i = 0; i < stockNodes.getLength(); i++) {
                Node stockNode = stockNodes.item(i);
                if (stockNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element stockElement = (Element) stockNode;
                    String stockSymbol = stockElement.getAttribute("symbol");
                    if (stockSymbol.equals(symbol)) {
                        String sharesText = stockElement.getTextContent();
                        return Integer.parseInt(sharesText);
                    }
                }
            }
            return 0;
        }
    
        public void setShares(String symbol, int shares) throws TransformerException {
            Element portfolioElement = document.getDocumentElement();
            NodeList stockNodes = portfolioElement.getElementsByTagName("stock");
    
            for (int i = 0; i < stockNodes.getLength(); i++) {
                Node stockNode = stockNodes.item(i);
                if (stockNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element stockElement = (Element) stockNode;
                    String stockSymbol = stockElement.getAttribute("symbol");
                    if (stockSymbol.equals(symbol)) {
                        stockElement.setTextContent(Integer.toString(shares));
                        break;
                    }
                }
            }
            save();
        }

        private void save() throws TransformerException {
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    
            DOMSource source = new DOMSource(document);
            StreamResult result = new StreamResult(new File(path));
            transformer.transform(source, result);
        }
    }

    private static class Strategy {
        Rule[] rules;
        Portfolio portfolio;

        public Strategy(String strategyPath, Portfolio portfolio) throws ParserConfigurationException, IOException, SAXException {
            this.portfolio = portfolio;

            DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            File strategyFile = new File(strategyPath);
            Document strategy = docBuilder.parse(strategyFile);
            NodeList ruleElems = strategy.getElementsByTagName("when");

            rules = new Rule[ruleElems.getLength()];
            for (int i = 0; i < ruleElems.getLength(); i++) {
                Element ruleElem = (Element) ruleElems.item(i);
                rules[i] = new Rule(ruleElem);
            }
        }

        public void evaluate(StockPrice[] stockPrices, Consumer<StockTransaction> placeTransaction) {
            for (StockPrice stockPrice: stockPrices) {
                for (Rule rule : rules) {
                    if (rule.signalRaised(stockPrice)) {
                        placeTransaction.accept(new StockTransaction(
                            rule.transaction.symbol,
                            rule.transaction.type,
                            rule.transaction.shares == -1 ? this.portfolio.getShares(rule.transaction.symbol) : rule.transaction.shares
                        ));
                    }
                }
            }
        }

        public String toString() {
            String out = "Strategy with rules:\n";
            for (Rule rule : rules) {
                out += rule.toString() + "\n";
            }
            return out;
        }
    }

    private static class Rule {
        private String symbol;
        private int above = -1; // -1 represents lack of ceiling
        private int below = -1; // -1 represents lack of floor

        public StockTransaction transaction;

        public Rule(Element rule) {
            // parse relevant symbol
            symbol = rule.getElementsByTagName("stock")
                .item(0)
                .getTextContent();

            // parse conditions
            try {
                above = Integer.parseInt(
                    rule.getElementsByTagName("above")
                        .item(0)
                        .getTextContent()
                    );
            } catch (NullPointerException e) {
                above = -1;
            }

            try {
                below = Integer.parseInt(
                    rule.getElementsByTagName("below")
                        .item(0)
                        .getTextContent()
                    );
            } catch (NullPointerException e) {
                below = -1;
            }
            
            // parse actions (buy and sell are considered mutually exclusive)
            NodeList buyElems = rule.getElementsByTagName("buy");
            if (buyElems.getLength() > 0) {
                int shares = Integer.parseInt(buyElems.item(0).getTextContent());
                transaction = new StockTransaction(symbol, "buy", shares);
            }

            NodeList sellElems = rule.getElementsByTagName("sell");
            if (sellElems.getLength() > 0) {
                try {
                    int shares = Integer.parseInt(sellElems.item(0).getTextContent());
                    transaction = new StockTransaction(symbol, "sell", shares);
                } catch (NumberFormatException e) {
                    transaction = new StockTransaction(symbol, "sell", -1); // sell all available shares
                }
            }
        }

        /**
         * Evaluate whether or not to place this rule's transaction given
         * the stock price. The trading signal is raised if the price meets
         * this rule's conditions.
         * @param price stock price
         * @return trading signal
         */
        public boolean signalRaised(StockPrice price) {
            if (price.symbol.equals(symbol)) {
                if (above == -1 || price.adjustedPriceCents > above) {
                    if (below == -1 || price.adjustedPriceCents < below) {
                        return true;
                    }
                }
            }
            return false;
        }

        public String toString() {
            String aboveStr = above == -1 ? "-inf" : String.valueOf(above);
            String belowStr = below == -1 ? "inf" : String.valueOf(below);
            return "If " + aboveStr + " < " + symbol + " < " + belowStr + " then " + transaction.toString();
        }
    }

    /**
     * Represents a buy or sell of some amount of a symbol's shares.
     * If the number of shares is -1, then sell all available shares
     */
    public static class StockTransaction {
        public String symbol;
        public String type; // either "buy" or "sell"
        public int shares;

        public StockTransaction(String symbol, String type, int shares) {
            this.symbol = symbol;
            this.type = type;
            this.shares = shares;
        }

        public String toString() {
            String out = type + " ";
            if (shares == -1) {
                out += "all";
            } else {
                out += String.valueOf(shares);
            }
            out += " shares";
            return out;
        }
    }

    /**
     * Represents a symbol's stock price in cents
     */
    private static class StockPrice {
        public String symbol;
        public int adjustedPriceCents;

        public StockPrice(String symbol, int adjustedPriceCents) {
            this.symbol = symbol;
            this.adjustedPriceCents = adjustedPriceCents;
        }
    }

    // Future<Message> incoming = nc.request("subject", "hello world".getBytes(StandardCharsets.UTF_8));
    // Message msg = incoming.get(500, TimeUnit.MILLISECONDS);
    // String response = new String(msg.getData(), StandardCharsets.UTF_8);
}