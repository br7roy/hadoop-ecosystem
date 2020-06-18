 /*
  * Package tk.tak
  * FileName: App2
  * Author:   Tak
  * Date:     18/11/26 20:33
  */
 package tk.tak.hive;

 import java.util.ArrayList;
import java.util.List;

 /**
  * @author Tak
  */
 public class App2 {
  public static void main(String[] args) throws InterruptedException {
   ThreadTool tool = ThreadTool.getONE();
   tool.start();

   List<Runnable> list = new ArrayList<>();
   for (int i = 0; i < 100; i++) {
    Runnable r = new Runnable() {
     @Override
     public void run() {

     }
    };
    list.add(r);
   }
   tool.addTask(list);

/*   ExecutorService executorService = Executors.newFixedThreadPool(100);
   List<Future<Object>> futures = executorService.invokeAll(list);
   int randomSize = ThreadLocalRandom.current().nextInt(futures.size());
   Future<Object> objectFuture = futures.get(randomSize);
   try {
    objectFuture.get();
   } catch (ExecutionException e) {
    e.printStackTrace();
   }
   executorService.shutdown();*/

  }
 }
