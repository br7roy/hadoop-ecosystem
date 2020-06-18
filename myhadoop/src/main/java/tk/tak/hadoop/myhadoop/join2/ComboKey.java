 /*
  * Package tk.tak.hadoop.myhadoop.join2
  * FileName: ComboKey
  * Author:   Tak
  * Date:     2018/10/23 20:12
  */
 package tk.tak.hadoop.myhadoop.join2;

 import org.apache.hadoop.io.WritableComparable;

 import java.io.DataInput;
 import java.io.DataOutput;
 import java.io.IOException;

 /**
  * @author Tak
  */
 public class ComboKey implements WritableComparable<ComboKey> {
	 private int customerId;
	 // customer-0, order-1
	 private int flag;

	 public ComboKey() {
	 }

	 public ComboKey(int customerId, int flag) {
		 this.customerId = customerId;
		 this.flag = flag;
	 }

	 public int getCustomerId() {
		 return customerId;
	 }

	 public void setCustomerId(int customerId) {
		 this.customerId = customerId;
	 }

	 public int getFlag() {
		 return flag;
	 }

	 public void setFlag(int flag) {
		 this.flag = flag;
	 }

	 @Override
	 public int compareTo(ComboKey o) {
		 int _id = o.getCustomerId();
		 int _flag = o.getFlag();
		 if (customerId != _id) {
			 return customerId - _id;
		 } else {
			 return flag - _flag;
		 }
	 }

	 @Override
	 public void write(DataOutput out) throws IOException {
		 out.writeInt(customerId);
		 out.writeInt(flag);
	 }

	 @Override
	 public void readFields(DataInput in) throws IOException {
		 this.customerId = in.readInt();
		 this.flag = in.readInt();
	 }

	 @Override
	 public String toString() {
		 return "{" + customerId + "," + flag + "}";
	 }

 }
