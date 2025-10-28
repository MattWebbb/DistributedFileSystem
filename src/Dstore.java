import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Dstore {

  int controllerPort;
  int timeout;
  String fileFolder;
  PrintWriter controllerOutput;

  AtomicInteger rebalanceStoreCount = new AtomicInteger(0);
  ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  public void initialiseFolder(String directoryName) {
    Path directory = Paths.get(directoryName);
    File directoryAsFile = directory.toFile();

    if (!directoryAsFile.exists() || !directoryAsFile.isDirectory()) {
      return;
    }

    for (File file : Objects.requireNonNull(directoryAsFile.listFiles())) {
      emptyDirectory(file);
    }
  }

  public void emptyDirectory(File file) {
    System.out.println("Deleting");
    if (file.isDirectory()) {
      File[] contents = file.listFiles();
      if (contents != null) {
        for (File f : contents) {
          emptyDirectory(f);
        }
      }
    }
    if (!file.delete()) {
      System.out.println("error deleting file");
    }
  }

  public static void main(String[] args){
    new Dstore().mainNonStatic(args);
  }

  public void mainNonStatic(String[] args){
    int port;
    try {
      port = Integer.parseInt(args[0]);
      controllerPort = Integer.parseInt(args[1]);
      timeout = Integer.parseInt(args[2]);
      fileFolder = args[3];
    } catch (Exception e){
      System.out.println("Error with datastore args");
      return;
    }

    initialiseFolder(fileFolder);

    try {
      new Thread(() -> handleControllerRequest(port)).start();
      ServerSocket serverSocket = new ServerSocket(port);
      while (true){
        Socket client = serverSocket.accept();
        new Thread(() -> handleClient(client)).start();
      }
    } catch (Exception e){
      e.printStackTrace();
    }
  }

  public void handleControllerRequest(int port){
    try {
      Socket controllerSocket = new Socket(InetAddress.getLoopbackAddress(), controllerPort);
      BufferedReader input = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
      controllerOutput = new PrintWriter(controllerSocket.getOutputStream(), true);
      controllerOutput.println(Protocol.JOIN_TOKEN + " " + port);
      String line;
      while((line = input.readLine()) != null) {
        System.out.println("LINE RECEIVED FROM CONTROLLER" + line);
        String[] commandAndParameters = line.split(" ");
        switch (commandAndParameters[0]) {
          case Protocol.REMOVE_TOKEN -> handleDelete(commandAndParameters[1], controllerOutput, controllerSocket);
          case Protocol.LIST_TOKEN -> handleList(controllerOutput, controllerSocket);
          case Protocol.REBALANCE_TOKEN -> handleRebalance(commandAndParameters, controllerOutput, controllerSocket);
          default -> System.out.println("Unexpected command received (Could possibly be file contents received outside of timeout)" + Arrays.toString(commandAndParameters));
        }
      }
    } catch (Exception e) { System.out.println("Conntection with controller dropped"); }
  }

  public synchronized void handleDelete(String fileName, PrintWriter output, Socket client){
    try {
      Path file = Paths.get(fileFolder, fileName);
      if (Files.exists(file)){
        file.toFile().delete();
        output.println(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
      } else {
        output.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      }
    } catch (Exception e) {
      System.out.println("Error deleting file: " + e);
    }
  }

  public synchronized void handleList(PrintWriter output, Socket client){
    try {
      System.out.println("LIST requested");
      Path directory = Paths.get(fileFolder);
      File file = directory.toFile();
      String[] fileList = file.list();
      StringBuilder fileListBuilder = new StringBuilder();
      if (fileList != null){
        for(String fileString : fileList){
          fileListBuilder.append(" ").append(fileString);
        }
      }
      output.println(Protocol.LIST_TOKEN + fileListBuilder);
    } catch (Exception e){
      System.out.println("Error compiling list: " + e);
    }
  }

  public void handleClient(Socket client){
    try{
      BufferedReader input = new BufferedReader(new InputStreamReader(client.getInputStream()));
      PrintWriter output = new PrintWriter(client.getOutputStream(), true);
      String line;
      while((line = input.readLine()) != null) {
        String[] commandAndParameters = line.split(" ");
        switch (commandAndParameters[0]){
          case Protocol.STORE_TOKEN -> handleStore(commandAndParameters[1], Integer.parseInt(commandAndParameters[2]), output, client, true);
          case Protocol.LOAD_DATA_TOKEN -> handleLoad(commandAndParameters[1], output, client);
          case Protocol.REBALANCE_STORE_TOKEN -> handleStore(commandAndParameters[1], Integer.parseInt(commandAndParameters[2]), output, client, false);
          default -> System.out.println("Unexpected command received (Could possibly be file contents received outside of timeout)" + commandAndParameters[0] + "S" + client);
        }
      }
    } catch (Exception e) {
      System.out.println("Connection with client dropped: " + e);
    }
  }

  public void handleStore(String fileName, int fileSize, PrintWriter output, Socket client, boolean acknowledgementFlag){
    try {
      output.println(Protocol.ACK_TOKEN);
      byte[] content = new byte[fileSize];
      try {
        client.setSoTimeout(timeout);
        client.getInputStream().readNBytes(content, 0, fileSize);
        Path path = Paths.get(fileFolder, fileName);
        synchronized (this) {
          Files.write(path, content);
          if (acknowledgementFlag) {
            controllerOutput.println(Protocol.STORE_ACK_TOKEN + " " + fileName);}
        }
      } catch (SocketTimeoutException e) {
        System.out.println("Timeout reached storing " + fileName + " on " + client);
      } finally {
        client.setSoTimeout(1800000);
      }
    } catch (Exception e){
      System.out.println("Error storing file: " + e);
    }
  }

  public synchronized void handleLoad(String fileName, PrintWriter output, Socket client){
    try {
      Path file = Paths.get(fileFolder, fileName);
      if (Files.exists(file)){
        byte[] fileContent = Files.readAllBytes(file);
        OutputStream outputStream = client.getOutputStream();
        outputStream.write(fileContent);
      } else {
        client.close();
      }
    } catch (Exception e) {
      System.out.println("Error loading file: " + e);
    }
  }

  public synchronized void handleRebalance(String[] filesToSendAndRemove, PrintWriter output, Socket client) {
    try {

      rebalanceStoreCount.set(0);

      String[] filesToSendAndRemoveList = Arrays.copyOfRange(filesToSendAndRemove, 1, filesToSendAndRemove.length);
      int numFilesToSend = Integer.parseInt(filesToSendAndRemoveList[0]);
      int arrayPos = 1;

      for (int i = 0; i < numFilesToSend; i++) {
        String fileName = filesToSendAndRemoveList[arrayPos];
        int numRecipients = Integer.parseInt(filesToSendAndRemoveList[arrayPos + 1]);
        for (int j = arrayPos + 2; j < arrayPos + 2 + numRecipients; j++) {
          int port = Integer.parseInt(filesToSendAndRemoveList[j]);
          new Thread(() -> sendFile(fileName, port, filesToSendAndRemove, output)).start();
          rebalanceStoreCount.getAndIncrement();
        }
        arrayPos += 2 + numRecipients;
      }

      if (rebalanceStoreCount.get() == 0) {
        rebalanceDelete(filesToSendAndRemove, output);
      } else {
        scheduler.schedule(() -> {
          synchronized (this) {
            if (rebalanceStoreCount.get() == 0) {
              rebalanceDelete(filesToSendAndRemove, output);
            }
          }
        }, timeout, TimeUnit.MILLISECONDS);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error rebalancing");
    }
  }

  public synchronized void rebalanceDelete(String[] filesToSendAndRemove, PrintWriter output){

    String[] filesToSendAndRemoveList = Arrays.copyOfRange(filesToSendAndRemove, 1, filesToSendAndRemove.length);
    int numFilesToSend = Integer.parseInt(filesToSendAndRemoveList[0]);
    int arrayPos = 1;
    for (int i = 0; i < numFilesToSend; i++) {
      int numRecipients = Integer.parseInt(filesToSendAndRemoveList[arrayPos + 1]);
      arrayPos += 2 + numRecipients;
    }
    List<String> deleteList = Arrays.stream(filesToSendAndRemoveList).toList().subList(arrayPos + 1, filesToSendAndRemoveList.length);

    boolean successfulDelete = true;
    for(String file : deleteList){
      Path fileToDelete = Paths.get(fileFolder, file);
      if (Files.exists(fileToDelete)) {
        successfulDelete = successfulDelete && fileToDelete.toFile().delete();
      }
    }
    if (successfulDelete) {
      output.println(Protocol.REBALANCE_COMPLETE_TOKEN);
    }
  }

  public void sendFile(String fileName, int port, String[] filesToSendAndRemove, PrintWriter output){
    try {
      Socket dataStoreClient = new Socket(InetAddress.getLoopbackAddress(), port);
      BufferedReader dataStoreInput = new BufferedReader(new InputStreamReader(dataStoreClient.getInputStream()));
      OutputStream dataStoreOutput = dataStoreClient.getOutputStream();
      PrintWriter dataStoreOutputWriter = new PrintWriter(dataStoreOutput, true);

      Path file = Paths.get(fileFolder, fileName);
      long fileSize = Files.size(file);
      dataStoreOutputWriter.println(Protocol.REBALANCE_STORE_TOKEN + " " + fileName + " " + fileSize);

      String response;
      try {
        dataStoreClient.setSoTimeout(timeout);
        response = dataStoreInput.readLine();
        synchronized (this) {
          if(response.equals(Protocol.ACK_TOKEN)){
            byte[] fileContent = Files.readAllBytes(file);
            dataStoreOutput.write(fileContent);
            rebalanceStoreCount.getAndDecrement();
            if (rebalanceStoreCount.get() == 0){
              rebalanceStoreCount.set(-1);
              rebalanceDelete(filesToSendAndRemove, output);
            }
          }
        }
      } catch (SocketTimeoutException ignored) {} finally {
        dataStoreClient.close();
        System.out.println(dataStoreClient.isClosed());
      }
    } catch (Exception e) {
      System.out.println("Error sending file to datastore: " + e);
    }
  }

}
