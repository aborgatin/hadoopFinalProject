package com.griddynamics.aborgatin.producer;

import com.google.common.net.InetAddresses;
import com.griddynamics.aborgatin.producer.classifiers.Product;
import org.apache.commons.net.util.SubnetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class RandomEventProducer {

    private static Logger LOGGER = LoggerFactory.getLogger(RandomEventProducer.class);

    private Socket clientSocket;
    private PrintWriter out;
    private static final String COLUMN_DELIMITER = ",";
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
    private Random rnd = new Random();

    private List<String> masks;
    {
        masks = new ArrayList<>();
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("GeoLite2-Country-Blocks-IPv4.csv").getFile());
        BufferedReader reader= null;
        try {
            reader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line = null;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            while(line!=null) {
                masks.add(line.split(",")[0]);
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void start(String ip, int port, int amount, int delay) throws IOException, InterruptedException {
        LOGGER.info("Producer is started");
        LOGGER.info("Port number is {}", port);
        clientSocket = new Socket(ip, port);
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        LOGGER.info("producer amount is {}", amount);
        int i = 0;
        Date today = new Date();
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -7);
        Date weekAgo = calendar.getTime();
        while (i < amount) {
            Product randomProduct = Product.values()[rnd.nextInt(Product.values().length)];
            Date date = new Date(ThreadLocalRandom.current().nextLong(weekAgo.getTime(), today.getTime()));
            String ipAddress = getIpAddress();
            out.println(new StringBuilder(randomProduct.getName())
                    .append(COLUMN_DELIMITER)
                    .append(randomProduct.getPrice())
                    .append(COLUMN_DELIMITER)
                    .append(sdf.format(date))
                    .append(COLUMN_DELIMITER)
                    .append(randomProduct.getCategory())
                    .append(COLUMN_DELIMITER)
                    .append(ipAddress));
            i++;
            Thread.sleep(delay);
        }
        LOGGER.info("Producer is finish");
    }

    private String getIpAddress() {
        int size = 0;
        SubnetUtils.SubnetInfo info = null;
        while (size == 0) {
            String mask = masks.get(rnd.nextInt(masks.size()));
            SubnetUtils utils = new SubnetUtils(mask);
            info = utils.getInfo();
            size = new Long(info.getAddressCountLong()).intValue();
        }
        return info.getAllAddresses()[rnd.nextInt(size)];

    }

    public void stop() {
        out.close();
        try {
            clientSocket.close();
        } catch (IOException e) {
            LOGGER.error("Error stopping client socket", e);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        String ip = "127.0.0.1";
        int port = 44445;
        int amount = 3000;
        int delay = 3;
        boolean needStart = true;
        switch (args.length) {
            case 4: {
                delay = Integer.parseInt(args[3]);
            }
            case 3: {
                ip = args[2];
            }
            case 2: {
                amount = Integer.parseInt(args[1]);
            }
            case 1: {
                if ("help".equals(args[0])) {
                    LOGGER.info("use optional params <port> <amount> <ip_address> <delay_in_millis>");
                    needStart = false;
                } else {
                    port = Integer.parseInt(args[0]);
                }
                break;
            }
            case 0: break;
            default: throw new IllegalArgumentException("Use from one to four optional parameters");
        }
        if (needStart) {
            RandomEventProducer randomEventProducer = new RandomEventProducer();
            try {
                randomEventProducer.start(ip, port, amount, delay);
            } catch (Exception e) {
                LOGGER.error("Error producing", e);
            } finally {
                randomEventProducer.stop();
            }
        }
    }
}
