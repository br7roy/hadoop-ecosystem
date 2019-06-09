package com.rust.hadoop.myhadoop;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

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

    public void testFileFormat() throws IOException {
        String root = "E:\\Developer\\project\\gitlab\\teqExp\\hadoop\\hadoop第十一天";
        Path path = Paths.get(root,"wc.txt");

        InputStream inputStream = Files.newInputStream(path, StandardOpenOption.READ);

        byte[] bs = new byte[1024];

        StringBuilder stringBuilder = new StringBuilder();
        int len = 0;
        while ((len = inputStream.read(bs)) != -1) {
            stringBuilder.append(new String(bs, 0, len));
        }
        String es = stringBuilder.toString();

        System.out.println(es);

        String[] s1 = es.split("\r\n");
        System.out.println(s1.length);
        Stream<String> stream = Stream.of(s1);

        stream.forEach(System.out::println);

        String[]s2 = es.split("(/s|\r|\n|\r\n)");
        Stream<String> stringStream = Stream.of(s2);
        stringStream.forEach(System.out::println);

        String[] ss = es.split(" |\r\n");
        System.out.println("<<<<<<<<<<<<<<<<<<<");
        System.out.println(Arrays.toString(ss));

        System.out.println("------------------");
        List<String> strings = new ArrayList<>();
        for (String s : ss) {
            if (s!=null) {
                String[] str = s.split("\r\n");
                strings.addAll(Arrays.asList(str));
            }
        }
        System.out.println(strings);

    }

}