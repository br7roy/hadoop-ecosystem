package com.rust.hadoop.myhadoop;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }


    public void test2() {
/*        int cap = 10;
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;

        System.out.println(n);*/

        int a = 12;
        int b = 19;
        System.out.println("a:"+a+",b:"+b);
        a = a ^ b; //a:31,b:19
        System.out.println("a:"+a+",b:"+b);
        b = a ^ b; //a:31,b:12
        System.out.println("a:"+a+",b:"+b);
        a = a ^ b; //a:19,b:12
        System.out.println("a:"+a+",b:"+b);

        System.out.println(-1 ^ 10);
    }


}