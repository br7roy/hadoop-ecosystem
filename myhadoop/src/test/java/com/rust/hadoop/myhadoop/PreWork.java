 /*
  * Package com.rust.hadoop.myhadoop
  * FileName: PreWork
  * Author:   Rust
  * Date:     2018/10/12 21:32
  */
 package com.rust.hadoop.myhadoop;

 import org.apache.poi.xssf.usermodel.XSSFRow;
 import org.apache.poi.xssf.usermodel.XSSFSheet;
 import org.apache.poi.xssf.usermodel.XSSFWorkbook;
 import org.junit.Test;

 import java.io.BufferedWriter;
 import java.io.IOException;
 import java.io.InputStream;
 import java.net.URI;
 import java.net.URISyntaxException;
 import java.net.URL;
 import java.nio.file.Files;
 import java.nio.file.Path;
 import java.nio.file.Paths;
 import java.util.Arrays;

 import static com.sun.org.apache.xml.internal.serialize.LineSeparator.Windows;

 /**
  * @author Rust
  */
 public class PreWork {
	 @Test
	 public void read() throws URISyntaxException, IOException {
		 URL url = Thread.currentThread().getContextClassLoader().getResource("20180928.xlsx");
		 URI uri = url.toURI();
		 System.out.println(uri.getPath().substring(1));
		 Path readPath = Paths.get(uri.getPath().substring(1));

/*		BufferedReader bufferedReader =
				new BufferedReader(new InputStreamReader(readPath.getFileSystem().provider().newInputStream(readPath),
						StandardCharsets.UTF_8.newDecoder()));*/
	 }

	 @Test
	 public void readAndTransfer() throws URISyntaxException, IOException {
		 URI rUri = Thread.currentThread().getContextClassLoader().getResource("20180928.xlsx").toURI();
		 URI outUri = getClass().getProtectionDomain().getCodeSource().getLocation().toURI().resolve(Paths.get(
				 "src/main/resources/output.log").toFile().toURI());

		 Path readPath = Paths.get(rUri.getPath().substring(1));
		 Path outputPath = Paths.get(outUri);
		 if (!Files.exists(outputPath)) {
			 Files.createFile(outputPath);
		 }

		 try (InputStream inputStream = Files.newInputStream(readPath);
			  BufferedWriter bufferedWriter = Files.newBufferedWriter(outputPath)) {

			 XSSFWorkbook workbook = new XSSFWorkbook(inputStream);

			 /*** step1: 获取Excel的工作区间总数*/
			 int sheetNo = workbook.getNumberOfSheets();//取得工作区间的个数（一个Excel默认的sheet有三个）
			 for (int i = 0; i < sheetNo; i++) {


				 /*** step2：取得所需工作区间(下标从0开始) */
				 XSSFSheet sheet = workbook.getSheetAt(i);
				 if (sheet == null) {
					 return;
				 }

				 /*** step3：获取总共有多少行数据因为中间空行的话，则读取出来的数据不准确 */
				 int hasRowNum = sheet.getPhysicalNumberOfRows();
				 if (hasRowNum == 0) {//sheet中所有行都没有内容
					 return;
				 }

				 //已经处理了的行数
				 int procssedNum = 0;
				 for (int j = 0; ; j++) {
					 /** step4: 获取每一行 */
					 XSSFRow row = sheet.getRow(j);
					 /** step5 : 去除空行 */
					 if (row != null) {
						 /** step6: 获取每一行的长度 */
						 int length = row.getLastCellNum();
						 if (length > 0) {
							 Object[] data = new Object[length];//定义一个集合，装每一行的数值
							 for (int m = 0; m < length; m++) {
								 /** step7: 获取每一行的每一列的值 */
								 data[m] = row.getCell(m);
							 }
							 /** step8: 存数据 */
							 if (procssedNum > 1) {
								 bufferedWriter.write(Arrays.toString(data).replaceAll("\\[|\\]", ""));
								 bufferedWriter.write(Windows);
							 }
						 }
						 procssedNum++;
						 if (procssedNum == hasRowNum) {//当处理完所有的数据，终止循环
							 break;
						 }
					 }
				 }
			 }
		 }
	 }

	 public static void main(String[] args) {
		 String s = "[";
		 String ss = "[寿险二级机构Vs行政区划 对照表, , ]";

		 String s1 = ss.replaceAll("\\[|\\]", "");
		 System.out.println(s1);
	 }
 }
