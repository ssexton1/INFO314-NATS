import io.nats.client.*;
import java.io.File;
import java.io.FileWriter;

public class StockMonitor {

    private static Connection nc = Nats.connect("nats://localhost:4222");

    public static void main(String[] args) {
        String[] stock_list = { "*" };
        if (args.length > 0) {
            stock_list = args;
        }
        createLogFiles(stock_list);

        Dispatcher stockMonitorDispatcher = nc.createDispatcher((msg) -> {
            try {
                logPriceAdjustment(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        // listen to stocks
        // create log files for each stock
        // add timestamp, adjustment, current price to file
    
    }

    private static void createLogFiles(String[] stock_list) {
        try {
            for (String stock : stock_list) {
                File stock_file = new File(stock + "-log.txt");
                stock_file.createNewFile();
            }
         catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void logPriceAdjustment(Message msg) {
        try {
            String subject = msg.getSubject();
            String stock_name = subject.substring(subject.indexOf(".") + 1);
            String adjustment = msg.getData();
            FileWriter stock_file = new FileWriter(stock_name + "-log.txt", true);
            stock_file.append(adjustment);
            stock_file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
