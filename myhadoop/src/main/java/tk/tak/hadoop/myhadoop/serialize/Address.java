package tk.tak.hadoop.myhadoop.serialize;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Address implements Writable {
	private String privince;
	private String city;
	private String street;

	public String getPrivince() {
		return privince;
	}

	public void setPrivince(String privince) {
		this.privince = privince;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.privince);
		out.writeUTF(this.city);
		out.writeUTF(this.street);
	}

	public void readFields(DataInput in) throws IOException {
		this.privince = in.readUTF();
		this.city = in.readUTF();
		this.street = in.readUTF();
	}

	@Override
	public String toString() {
		return "Address [privince=" + privince + ", city=" + city + ", street=" + street
				+ "]";
	}

}
