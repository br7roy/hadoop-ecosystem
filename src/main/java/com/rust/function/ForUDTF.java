 /*
  * Package com.rust.function
  * FileName: ForUDTF
  * Author:   Takho
  * Date:     18/12/10 21:03
  */
 package com.rust.function;

 import org.apache.hadoop.hive.ql.exec.Description;
 import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
 import org.apache.hadoop.hive.ql.metadata.HiveException;
 import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
 import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
 import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
 import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
 import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
 import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
 import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
 import org.apache.hadoop.io.IntWritable;

 import java.util.ArrayList;

 /**
  * 简单for循环函数
  *
  * @author Takho
  */
 @Description(name = "forx", value = "this is my first UDTF!", extended = "e.g : select forx(1,5,1);")
 public class ForUDTF extends GenericUDTF {
	 private IntWritable start;// 起始
	 private IntWritable end;// 结束
	 private IntWritable inc;// 增量
	 private Object[] forwardObj;


	 @Override
	 public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
		 start = ((WritableConstantIntObjectInspector) argOIs[0]).getWritableConstantValue();
		 end = ((WritableConstantIntObjectInspector) argOIs[1]).getWritableConstantValue();
		 if (argOIs.length == 3) {
			 inc = ((WritableConstantIntObjectInspector) argOIs[2]).getWritableConstantValue();
		 } else
			 inc = new IntWritable(1);
		 this.forwardObj = new Object[1];
		 ArrayList<String> fieldNames = new ArrayList<>();
		 ArrayList<ObjectInspector> fieldOis = new ArrayList<>();
		 fieldNames.add("col0");
		 fieldOis.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.INT));
		 return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOis);
	 }

	 @Override
	 public void process(Object[] objects) throws HiveException {

		 for (int i = start.get(); i < end.get(); i = i + inc.get()) {
			 this.forwardObj[0] = new Integer(i);
			 forward(forwardObj);
		 }


	 }

	 @Override
	 public void close() throws HiveException {

	 }
 }
