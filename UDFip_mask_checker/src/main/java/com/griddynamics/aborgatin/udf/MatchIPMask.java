package com.griddynamics.aborgatin.udf;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

public final class MatchIPMask extends UDF {

    public BooleanWritable evaluate(final Text ip, final Text mask) {
        SubnetUtils utils = new SubnetUtils(mask.toString());
        SubnetUtils.SubnetInfo info = utils.getInfo();
        return new BooleanWritable(info.isInRange(ip.toString()));
    }
}
