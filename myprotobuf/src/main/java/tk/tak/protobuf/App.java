package tk.tak.protobuf;

import sun.nio.cs.ext.Johab;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )throws Exception
    {

        AddressBookProtos.Person john = AddressBookProtos.Person.newBuilder()
                .setId(12345)
                .setName("john")
                .setEmail("222@gamil.com")
                .addPhone(AddressBookProtos.Person.PhoneNumber.newBuilder()
                .setNumber("+8618516600266")
                .setType(AddressBookProtos.Person.PhoneType.HOME)
                .build())
                .build();
        System.out.println(Johab.defaultCharset());
        // 串行
        john.writeTo(new FileOutputStream("E:\\Developer\\bigdata\\protobuf\\person.data"));

        // 反串行
        AddressBookProtos.Person person = AddressBookProtos.Person.parseFrom(new FileInputStream("E:\\Developer\\bigdata\\protobuf\\person.data"));
        System.out.println(person.getEmail());


    }
}
