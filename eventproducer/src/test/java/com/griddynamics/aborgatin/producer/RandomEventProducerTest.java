package com.griddynamics.aborgatin.producer;

import com.google.common.net.InetAddresses;
import com.griddynamics.aborgatin.producer.classifiers.ProductCategory;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class RandomEventProducerTest {
    private static final int PORT_DEFAULT = 44445;
    private static final int AMOUNT_DEFAULT = 3000;
    private static final String IP_DEFAULT = "127.0.0.1";

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");

    @Test
    public void testMain() throws IOException {
        final String[] args = new String[] {String.valueOf(PORT_DEFAULT), String.valueOf(AMOUNT_DEFAULT), IP_DEFAULT, "0"};
        ServerSocket serverSocket = new ServerSocket(PORT_DEFAULT);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    RandomEventProducer.main(args);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        Socket clientSocket = serverSocket.accept();

        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
            String message = in.readLine();
            int count = 0;
            Date today = new Date();
            Calendar calendar = new GregorianCalendar();
            calendar.add(Calendar.DAY_OF_MONTH, -7);
            Date weekAgo = calendar.getTime();
            while (message != null) {
                Assert.assertNotNull(message);
                String[] arr = message.split(",");
                Assert.assertEquals(5, arr.length);

                //assert product name field
                Assert.assertNotNull(arr[0]);

                //assert cost field
                try {
                    Float cost = Float.parseFloat(arr[1]);
                    Assert.assertNotNull(cost);
                } catch (NumberFormatException e) {
                    throw new AssertionError("Cost is not float");
                }

                //assert purchase date field
                try {
                    Date date = sdf.parse(arr[2]);
                    Assert.assertNotNull(date);
                    Assert.assertTrue(date.compareTo(today) <= 0);
                    Assert.assertTrue(date.compareTo(weekAgo) >= 0);
                } catch (ParseException e) {
                    throw new AssertionError("Date doesn`t corresponds format \"yyyy-MM-dd HH:mm:ssZ\"");
                }

                //assert product category
                Assert.assertNotNull(ProductCategory.valueOf(arr[3]));

                //assert ip address
                Assert.assertTrue(InetAddresses.isInetAddress(arr[4]));

                message = in.readLine();
                count++;
            }
            Assert.assertEquals(AMOUNT_DEFAULT, count);
        }
    }
}
