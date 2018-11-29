package com.griddynamics.aborgatin.udf;

import com.google.common.base.Strings;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FindCountryUDF extends UDF {

    private static List<Mask> blocks = new ArrayList<Mask>();
    private static Map<Integer, String> locations = new HashMap<Integer, String>();



    static {
        ClassLoader classLoader = FindCountryUDF.class.getClassLoader();
        InputStream blocksInputStream  = classLoader.getResourceAsStream("GeoLite2-Country-Blocks-IPv4.csv");
        InputStream locationInputStream = classLoader.getResourceAsStream("GeoLite2-Country-Locations-en.csv");

        try {
        List<String> locationList = null;
            locationList = IOUtils.readLines(locationInputStream);

        for (String location :locationList) {
            String[] arr = location.split(",");
            locations.put(Integer.valueOf(arr[0]), arr[5]);
        }
        List<String> blockList = IOUtils.readLines(blocksInputStream);
        for (String block : blockList) {
            String[] arr = block.split(",");
            if (!Strings.isNullOrEmpty(arr[2])) {
                blocks.add(new Mask(covertToLowBound(arr[0]),covertToHighBound(arr[0]), Integer.valueOf(arr[2])));
            }
        }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }}

    private static Long covertToHighBound(String mask) {
        SubnetUtils subnetUtils = new SubnetUtils(mask);
        SubnetUtils.SubnetInfo info = subnetUtils.getInfo();
        return convertIpToNumber(info.getBroadcastAddress());
    }

    private static Long covertToLowBound(String mask) {
        SubnetUtils subnetUtils = new SubnetUtils(mask);
        SubnetUtils.SubnetInfo info = subnetUtils.getInfo();
        return convertIpToNumber(info.getNetworkAddress());
    }

    private static Long convertIpToNumber(String ip) {
        String[] ipArr = ip.split("\\.");
        StringBuilder res = new StringBuilder() ;
        for (String dec : ipArr) {
            String decBinary = Integer.toBinaryString(Integer.parseInt(dec));
            if (dec.length() < 8) {
                decBinary = new String(new char[8 - decBinary.length()]).replace("\0", "0") + decBinary;
            }
            res.append(decBinary);
        }
        return Long.parseLong(res.toString(), 2);
    }

    public Text evaluate(final Text ip) {
        long longIp = convertIpToNumber(ip.toString());

        int left = 0;
        int right = blocks.size();
        int mid = 0;
        while (!(left >= right)) {
            mid = left + (right - left)/2;
            if (blocks.get(mid).inRange(longIp)) {
                return new Text(locations.get(blocks.get(mid).getCountryId()));
            }
            if (blocks.get(mid).getIpStart() > longIp)
                right = mid;
            else
                left = mid + 1;
        }
        mid = left - 1;
        if (mid > 0 && longIp >= blocks.get(mid).getIpStart() && longIp <= blocks.get(mid).getIpEnd())
            return new Text(locations.get(blocks.get(mid).getCountryId()));
        else
            return null;
    }


    private static final class Mask {
        private long ipStart;
        private long ipEnd;
        private int countryId;

        public Mask(long ipStart, long ipEnd, int countryId) {
            this.ipStart = ipStart;
            this.ipEnd = ipEnd;
            this.countryId = countryId;
        }

        public boolean inRange(long ip) {
            return ip >= ipStart && ip <= ipEnd;
        }

        public int getCountryId() {
            return countryId;
        }

        public long getIpStart() {
            return ipStart;
        }

        public long getIpEnd() {
            return ipEnd;
        }
    }

    public static void main(String[] args) throws IOException {
        FindCountryUDF findCountryUDF = new FindCountryUDF();
        System.out.println(findCountryUDF.evaluate(new Text(args[0])));
    }
}
