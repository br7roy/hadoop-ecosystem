package com.rust.flume;

import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.Assert.assertTrue;

//
///////////////////////////////////////////////////////////////////////////

/**
 * Unit test for simple App.
 */
public class AppTest {
	/**
	 * Rigorous Test :-)
	 */
	@Test
	public void shouldAnswerWithTrue() {
		assertTrue(true);
	}


	@Test
	public void sendHttp() throws Exception {
		URL url = new URL("http://s100:8888");
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setDoOutput(true); //设置输入流（Post时必须设置）
		connection.setDoInput(true); // 设置输出流（Post时必须设置）
		connection.setRequestMethod("POST"); // 设置方式为POST（Post时必须设置）
		connection.setRequestProperty("Content-Type", "application/json");
		OutputStream os = connection.getOutputStream();
		String msg = "[{\"body\":\"haaaaaaaaaaaaaa\"}]";
		os.write(msg.getBytes());
		os.flush();

		InputStream inputStream = connection.getInputStream();
		byte[] bs = new byte[1024];
		inputStream.read(bs);
		System.out.println(new String(bs));
		connection.disconnect(); //关闭HttpURLConnection连接

	}
}
