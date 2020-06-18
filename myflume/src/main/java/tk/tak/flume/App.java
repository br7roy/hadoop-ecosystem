package tk.tak.flume;

import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.FileInputStream;
import java.util.concurrent.Phaser;

/**
 * Hello world!
 */
public class App {
	public static void main(String[] args) throws Exception {
		Phaser p = new Phaser(1);
		String conn = "s101:2181,s102:2181,s103:2181";
		ZooKeeper zk = new ZooKeeper(conn, 5000, event -> {
			System.out.println("over:" + event);
			if (event.getState() == KeeperState.SyncConnected) {
				p.arrive();
			}
		});
		p.awaitAdvance(p.getPhase());
		Stat stat = new Stat();
		zk.getData("/flume/a1", false, stat);
		int v = stat.getVersion();
		FileInputStream fis = new FileInputStream("e:/avro_r.conf");
		byte[] bs = new byte[fis.available()];
		fis.read(bs);
		fis.close();
		zk.setData("/flume/a1", bs, v);
		zk.close();

	}
}
