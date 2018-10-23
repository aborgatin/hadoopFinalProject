package com.griddynamics.aborgatin.producer;

import com.google.common.net.InetAddresses;
import com.griddynamics.aborgatin.producer.classifiers.Product;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
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
        Random rnd = new Random();
        while (i < amount) {
            Product randomProduct = Product.values()[rnd.nextInt(Product.values().length)];
            Date date = new Date(ThreadLocalRandom.current().nextLong(weekAgo.getTime(), today.getTime()));
            String ipAddress = InetAddresses.fromInteger(rnd.nextInt()).getHostAddress();
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

    public void stop() {
        out.close();
        try {
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
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
                LOGGER.error(e.getMessage());
            } finally {
                randomEventProducer.stop();
            }
        }
    }
}
