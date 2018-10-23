package com.griddynamics.aborgatin.udf;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class MatchIPMaskTest {

    @Test
    public void testEvaluate() {
        MatchIPMask matchIPMask = new MatchIPMask();
        Assert.assertTrue(matchIPMask.evaluate(new Text("10.10.10.123"), new Text("10.10.10.0/24")).get());
        Assert.assertTrue(matchIPMask.evaluate(new Text("10.10.123.123"), new Text("10.10.10.0/16")).get());
        Assert.assertTrue(matchIPMask.evaluate(new Text("10.85.123.123"), new Text("10.10.10.0/8")).get());
        Assert.assertFalse(matchIPMask.evaluate(new Text("10.85.123.123"), new Text("10.10.10.0/24")).get());
        Assert.assertFalse(matchIPMask.evaluate(new Text("10.85.123.123"), new Text("10.10.10.0/16")).get());

    }

}
