import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;
import javax.swing.plaf.TableHeaderUI;

public class ClientMain {
	
	public static void main(String[] args) throws Exception{
		
		final int cport = 12345;
		int timeout = 1000;

		String[] argsController = {"12345", "2", "1000", "2000"};
		new Thread(() -> Controller.main(argsController)).start();

		Thread.sleep(1000);

		String[] args1 = {"12346", "12345", "1000", "folder1"};
		new Thread(() -> Dstore.main(args1)).start();

		String[] args2 = {"12347", "12345", "1000", "folder2"};
		new Thread(() -> Dstore.main(args2)).start();

		String[] args3 = {"12348", "12345", "1000", "folder3"};
		new Thread(() -> Dstore.main(args3)).start();
		
		// this client expects a 'downloads' folder in the current directory; all files loaded from the store will be stored in this folder
		File downloadFolder = new File("downloads");
		if (!downloadFolder.exists())
			if (!downloadFolder.mkdir()) throw new RuntimeException("Cannot create 'downloads' folder (folder absolute path: " + downloadFolder.getAbsolutePath() + ")");
		
		// this client expects a 'to_store' folder in the current directory; all files to be stored in the store will be collected from this folder
		File uploadFolder = new File("to_store");
		if (!uploadFolder.exists())
			if (!uploadFolder.mkdir()) throw new RuntimeException("Cannot create 'to_store' folder (folder absolute path: " + uploadFolder.getAbsolutePath() + ")");
		
		//launch a single client
		testClient(cport, timeout, downloadFolder, uploadFolder);
		
		//launch a number of concurrent clients, each doing the same operations
		for (int i = 0; i < 5; i++) {
			new Thread() {
				public void run() {
					//test2Client(cport, timeout, downloadFolder, uploadFolder);
				}
			}.start();
		}
	}
	
	public static void test2Client(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;
		
		try {
			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
			client.connect();
			Random random = new Random(System.currentTimeMillis() * System.nanoTime());

			File fileList[] = uploadFolder.listFiles();


			try { client.store(fileList[0]); } catch (Exception e) {e.printStackTrace();}


			//try { Thread.sleep(4000); } catch (Exception ignored) {}


			String list[] = null;
			//try { list = list(client); } catch(IOException e) { e.printStackTrace(); }

			try { Thread.sleep(4000); } catch (Exception ignored) {}

			try { client.load(fileList[0].getName(), downloadFolder); } catch(IOException e) { e.printStackTrace(); }

			try { Thread.sleep(4000); } catch (Exception ignored) {}


			try { client.remove("Masterpiece.png"); } catch(IOException e) { e.printStackTrace(); }

			try { Thread.sleep(1000000); } catch (Exception ignored) {}



			for (int i=0; i<fileList.length/2; i++) {
				File fileToStore = fileList[random.nextInt(fileList.length)];
				try {					
					client.store(fileToStore);
				} catch (Exception e) {
					System.out.println("Error storing file " + fileToStore);
					e.printStackTrace();
				}
			}

			

			
			for (int i = 0; i < list.length/4; i++) {
				String fileToRemove = list[random.nextInt(list.length)];
				try {
					client.remove(fileToRemove);
				} catch (Exception e) {
					System.out.println("Error remove file " + fileToRemove);
					e.printStackTrace();
				}
			}
			
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}
	
	public static void testClient(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;
		
		try {

			Thread.sleep(1000);

			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
			try { client.connect(); } catch(IOException e) { e.printStackTrace(); return; }

			Thread.sleep(1000);

			//try { list(client); } catch(IOException e) { e.printStackTrace(); }

			//Thread.sleep(1000);

			File fileList[] = uploadFolder.listFiles();

			try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
			try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
			try { client.store(fileList[2]); } catch(IOException e) { e.printStackTrace(); }
			//try { client.store(fileList[2]); } catch(IOException e) { e.printStackTrace(); }

			Thread.sleep(1000);

			String[] args4 = {"12351", "12345", "1000", "folder4"};
			new Thread(() -> Dstore.main(args4)).start();

			//try { list(client); } catch(IOException e) { e.printStackTrace(); }

			Thread.sleep(1000);



			//try { client.wrongLoad("Masterpiece.png", 4); } catch(IOException e) { e.printStackTrace(); }

			Thread.sleep(1000);


			//try { client.wrongLoad("Masterpiece.png", 4); } catch(IOException e) { e.printStackTrace(); }

			Thread.sleep(10000000);





			//Scanner scn = new Scanner(System.in);
			//System.out.print("Enter First Number: ");
			//String a = scn.nextLine();

			Thread.sleep(1000);


			try { client.remove("Masterpiece.png"); } catch(IOException e) { e.printStackTrace(); }

			Thread.sleep(1000);

			try { list(client); } catch(IOException e) { e.printStackTrace(); }

			//try { client.wrongLoad("Masterpiece.png", 4); } catch(IOException e) { e.printStackTrace(); }



			//System.out.print("--------------------------------------------------------------------------------");

			//String[] args5 = {"12350", "12345", "1000", "folder5"};
			//new Thread(() -> Dstore.main(args5)).start();

			//try { list(client); } catch(IOException e) { e.printStackTrace(); }


			//
			//try { client.remove("Picture.png"); } catch(IOException e) { e.printStackTrace(); }



			Thread.sleep(1000000);
			//	if (fileList.length > 0) {
			//	try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
			//	try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
			//}
			//if (fileList.length > 1) {
			//	try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
			//}

			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
			if (list != null)
				for (String filename : list)
					try { client.load(filename, downloadFolder); } catch(IOException e) { e.printStackTrace(); }
			
			if (list != null)
				for (String filename : list)
					try { client.remove(filename); } catch(IOException e) { e.printStackTrace(); }
			if (list != null && list.length > 0)
				try { client.remove(list[0]); } catch(IOException e) { e.printStackTrace(); }
			
			try { list(client); } catch(IOException e) { e.printStackTrace(); }
			
		} catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}

	public static String[] list(Client client) throws IOException, NotEnoughDstoresException {
		System.out.println("Retrieving list of files...");
		String list[] = client.list();
		
		System.out.println("Ok, " + list.length + " files:");
		int i = 0; 
		for (String filename : list)
			System.out.println("[" + i++ + "] " + filename);
		
		return list;
	}
	
}
