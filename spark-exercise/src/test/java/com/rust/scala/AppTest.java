package com.rust.scala;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );

        String s = "sdfsdf";
        String[] ss = s.split(",", 2);
        System.out.println(Arrays.toString(ss));
    }
}
