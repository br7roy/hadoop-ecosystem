package tk.tak.hadoop.myhadoop.compress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Decompress {

	public static final Log LOG = LogFactory.getLog(Decompress.class.getName());

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String name = "io.compression.codecs";
		String value = "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,org.apache.hadoop.io.compress.BZip2Codec";
		conf.set(name, value);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		for (int i = 0; i < args.length; ++i) {
			CompressionCodec codec = factory.getCodec(new Path(args[i]));
			try (BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get("/home/tmp/test/otherRdd.txt"), StandardCharsets.UTF_8); CompressionInputStream in = codec.createInputStream(new java.io.FileInputStream(args[i]))) {
				if (codec == null) {
					System.out.println("Codec for " + args[i] + " not found.");
				} else {
					byte[] buffer = new byte[100];
					int len = in.read(buffer);
					while (len > 0) {
						System.out.write(buffer, 0, len);
						bufferedWriter.write(new String(buffer), 0, len);
						len = in.read(buffer);
					}
				}
			}
		}
	}
}
