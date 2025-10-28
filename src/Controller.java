import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Controller {

  static Index index = new Index();
  static Index newIndex = new Index();

  static HashMap<Integer, ArrayList<String>> filesToDelete = new HashMap<>();
  static HashMap<Integer, HashMap<String, ArrayList<Integer>>> filesToSend = new HashMap<>();

  static ArrayList<Integer> portsInUse = new ArrayList<>();
  static ConcurrentHashMap<Integer, Socket> dataStoreList = new ConcurrentHashMap<>();
  static ConcurrentHashMap<Socket, Integer> reverseDataStoreList = new ConcurrentHashMap<>();

  static ConcurrentHashMap<String, AcknowledgementTracker> currentStorePorts = new ConcurrentHashMap<>();
  static ConcurrentHashMap<Socket, HashMap<String, ArrayList<Integer>>> currentLoadPorts = new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, AcknowledgementTracker> currentRemovePorts = new ConcurrentHashMap<>();
  static ConcurrentHashMap<Integer, String> fileLists;

  static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(20);
  static ScheduledFuture<?> scheduledFuture;

  static CountDownLatch latch;
  static CountDownLatch latch2;

  static int replicationFactor;
  static int timeout;
  static int rebalancePeriod;

  public static void main (String[] args){

    int controllerPort;

    try {
      controllerPort = Integer.parseInt(args[0]);
      replicationFactor = Integer.parseInt(args[1]);
      timeout = Integer.parseInt(args[2]);
      rebalancePeriod = Integer.parseInt(args[3]);
    } catch (Exception e) {
      System.out.println("Error with controller args");
      return;
    }

    index.clear();
    scheduleNextRebalance();

    try {
      ServerSocket serverSocket = new ServerSocket(controllerPort);
      while (true){
        Socket client = serverSocket.accept();
        new Thread(() -> handleRequest(client)).start();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void handleRequest(Socket client){
    try {

      BufferedReader input = new BufferedReader(new InputStreamReader(client.getInputStream()));
      PrintWriter output = new PrintWriter(client.getOutputStream(), true);
      String line;

      while ((line = input.readLine()) != null) {
        System.out.println("Connection recieved" + client);
        String[] commandAndParameters = line.split(" ");
        switch (commandAndParameters[0]) {
          case Protocol.STORE_TOKEN -> handleStore(commandAndParameters[1], Integer.parseInt(commandAndParameters[2]), output, client);
          case Protocol.STORE_ACK_TOKEN -> handleStoreAcknowledgement(commandAndParameters[1], output, client);
          case Protocol.LOAD_TOKEN -> handleLoad(commandAndParameters[1], output, client);
          case Protocol.RELOAD_TOKEN -> handleReload(commandAndParameters[1], output, client);
          case Protocol.REMOVE_TOKEN -> handleRemove(commandAndParameters[1], output, client);
          case Protocol.REMOVE_ACK_TOKEN, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN -> handleRemoveAcknowledgement(commandAndParameters[1], output, client);
          case Protocol.JOIN_TOKEN -> handleJoin(Integer.parseInt(commandAndParameters[1]), client);
          case Protocol.LIST_TOKEN -> { if (!reverseDataStoreList.containsKey(client)) { handleListClient(output); } else { handleListServer(line, client); }}
          case Protocol.REBALANCE_COMPLETE_TOKEN -> handleRebalanceComplete(client);
          default -> System.out.println("Unexpected command received from " + client + ": " + commandAndParameters[0]);
        }
      }
      handleFailedDataStore(client);
    } catch (Exception e) { e.printStackTrace(); }
  }

  public static synchronized void handleFailedDataStore(Socket client){
    if (reverseDataStoreList.containsKey(client)){
      int port = reverseDataStoreList.get(client);
      System.out.println("datastore: " + port + " failed");

      portsInUse.remove(Integer.valueOf(port));

      // No idea if this should be done
      //dataStoreList.remove(port);
      //reverseDataStoreList.remove(client);
      //index.removePort(port);
    }
  }

  private static synchronized void scheduleNextRebalance() {
    if (scheduledFuture != null && !scheduledFuture.isDone()) {
      scheduledFuture.cancel(false);
    }
    scheduledFuture = scheduler.schedule(() -> {
      if (!checkEnoughDataStores()) { new Thread(Controller::startRebalance).start(); } else { scheduleNextRebalance(); }
    }, rebalancePeriod, TimeUnit.SECONDS);
  }

  public static synchronized void handleStore(String fileName, int fileSize, PrintWriter output, Socket client){ // Try and synchronize this
    try {
      for (String file : index.getFiles()){
        System.out.println(file + index.getDataStores(file));
      }
      if (index.contains(fileName)){
        HashMap<String, ArrayList<Integer>> listsOfPort = currentLoadPorts.getOrDefault(client, new HashMap<>());
        listsOfPort.put(fileName, index.getDataStores(fileName));
        currentLoadPorts.put(client, listsOfPort);
      }
      if (storeFile(fileName, fileSize, output, client)){
        scheduler.schedule(() -> {
          synchronized (AcknowledgementTracker.class) {
            if (currentStorePorts.containsKey(fileName)) {
              index.remove(fileName);
              currentStorePorts.remove(fileName);
            }
          }
        }, timeout, TimeUnit.MILLISECONDS);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error running store command");
    }
  }

  public static synchronized boolean storeFile(String fileName, int fileSize, PrintWriter output, Socket client){
    if (checkEnoughDataStores()){
      output.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      return false;
    }
    if(index.contains(fileName)) {
      output.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
      return false;
    }

    index.addValue(fileName, Protocol.STORING, fileSize, new ArrayList<>());
    ArrayList<Integer> leastUsed = getLeastUsedStore();
    ArrayList<Integer> portList = new ArrayList<>(leastUsed.subList(0, replicationFactor));

    StringBuilder portString = new StringBuilder();
    for(Integer port : portList){
      index.addDataStore(fileName, port);
      portString.append(" ").append(port);
    }

    currentStorePorts.put(fileName, new AcknowledgementTracker(client, portList));
    output.println(Protocol.STORE_TO_TOKEN + portString);

    return true;
  }

  public static void handleStoreAcknowledgement(String fileName, PrintWriter output, Socket client){
    try {
      if (index.contains(fileName)){
        HashMap<String, ArrayList<Integer>> listsOfPort = currentLoadPorts.getOrDefault(client, new HashMap<>());
        listsOfPort.put(fileName, index.getDataStores(fileName));
        currentLoadPorts.put(client, listsOfPort);
      }
      if(!currentStorePorts.containsKey(fileName)){
        System.out.println("Store ack received past timeout");
        return;
      }

      int port = reverseDataStoreList.get(client);
      System.out.println("Received ack from " + port);

      synchronized (AcknowledgementTracker.class){
        AcknowledgementTracker acknowledgementTracker = currentStorePorts.get(fileName);
        Socket sender = acknowledgementTracker.getSender();
        ArrayList<Integer> ports = new ArrayList<>(acknowledgementTracker.getPorts());

        ports.remove(Integer.valueOf(port));
        acknowledgementTracker.setPorts(ports);

        if(acknowledgementTracker.getPorts().isEmpty()){
          index.updateStatus(fileName, Protocol.STORED);
          currentStorePorts.remove(fileName);

          PrintWriter senderOutput = new PrintWriter(sender.getOutputStream(), true);
          senderOutput.println(Protocol.STORE_COMPLETE_TOKEN);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error receiving acknowledgement:  " + e);
    }
  }

  public static synchronized void handleLoad(String fileName, PrintWriter output, Socket client){

    if (index.contains(fileName)){
      HashMap<String, ArrayList<Integer>> listsOfPort = currentLoadPorts.getOrDefault(client, new HashMap<>());
      listsOfPort.put(fileName, index.getDataStores(fileName));
      currentLoadPorts.put(client, listsOfPort);
    }
    handleReload(fileName, output, client);
  }

  public static synchronized void handleReload(String fileName, PrintWriter output, Socket client){
    if (checkEnoughDataStores()){
      output.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      return;
    }
    ConcurrentHashMap<String, String> statusList = index.getStatusList();
    if (!statusList.containsKey(fileName)) {
      output.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      return;
    }
    if (!statusList.get(fileName).equals(Protocol.STORED)){
      output.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      return;
    }
    HashMap<String, ArrayList<Integer>> listsOfPort = currentLoadPorts.getOrDefault(client, new HashMap<>());
    if(!listsOfPort.containsKey(fileName)){
      System.out.println("Reload for non loaded file");
      return;
    }

    ArrayList<Integer> possiblePorts = listsOfPort.get(fileName);

    if(possiblePorts.isEmpty()){
      listsOfPort.remove(fileName);
      output.println(Protocol.ERROR_LOAD_TOKEN);
      return;

    } else {
      int port = possiblePorts.remove(0);
      int size = index.getSize(fileName);
      listsOfPort.put(fileName, possiblePorts);
      output.println(Protocol.LOAD_FROM_TOKEN + " " + port + " " + size);
    }

    currentLoadPorts.put(client, listsOfPort);
  }

  public static synchronized void handleRemove(String fileName, PrintWriter output, Socket client){
    try {
      if (index.contains(fileName)){
        HashMap<String, ArrayList<Integer>> listsOfPort = currentLoadPorts.getOrDefault(client, new HashMap<>());
        listsOfPort.put(fileName, index.getDataStores(fileName));
        currentLoadPorts.put(client, listsOfPort);
      }
      if (removeFiles(fileName, output, client)){
        scheduler.schedule(() -> {
          synchronized (AcknowledgementTracker.class) {
            currentRemovePorts.remove(fileName);
          }
        }, timeout, TimeUnit.MILLISECONDS);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error running remove command: " + e);
    }
  }

  public static synchronized boolean removeFiles(String fileName, PrintWriter output, Socket client) {
    try {
      if (checkEnoughDataStores()){
        output.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        return false;
      }
      ConcurrentHashMap<String, String> statusList = index.getStatusList();
      if (!statusList.containsKey(fileName)) {
        output.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        return false;
      }
      if (!statusList.get(fileName).equals(Protocol.STORED)){
        output.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        return false;
      }

      index.updateStatus(fileName, Protocol.REMOVING);
      System.out.println("INDEX.GETDATASTORES" + fileName + index.getDataStores(fileName));
      ArrayList<Integer> portList = index.getDataStores(fileName);
      currentRemovePorts.put(fileName, new AcknowledgementTracker(client, new ArrayList<>(portList)));

      for (int port : portList) {
        System.out.println("Deleting from: " + port);
        Socket socket = dataStoreList.get(port);
        PrintWriter dataStoreOutput = new PrintWriter(socket.getOutputStream(), true);
        dataStoreOutput.println(Protocol.REMOVE_TOKEN + " " + fileName);
      }
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error removing files");
      return false;
    }
  }

  public static void handleRemoveAcknowledgement(String fileName, PrintWriter output, Socket client){
    try {
      if (index.contains(fileName)){
        HashMap<String, ArrayList<Integer>> listsOfPort = currentLoadPorts.getOrDefault(client, new HashMap<>());
        listsOfPort.put(fileName, index.getDataStores(fileName));
        currentLoadPorts.put(client, listsOfPort);
      }
      if(!currentRemovePorts.containsKey(fileName)){
        System.out.println("Remove ack received past timeout");
        return;
      }

      int port = reverseDataStoreList.get(client);

      synchronized (AcknowledgementTracker.class){
        AcknowledgementTracker acknowledgementTracker = currentRemovePorts.get(fileName);
        Socket sender = acknowledgementTracker.getSender();
        ArrayList<Integer> ports = new ArrayList<>(acknowledgementTracker.getPorts());

        ports.remove(Integer.valueOf(port));
        acknowledgementTracker.setPorts(ports);

        if(acknowledgementTracker.getPorts().isEmpty()){
          index.remove(fileName);
          currentRemovePorts.remove(fileName);
          PrintWriter senderOutput = new PrintWriter(sender.getOutputStream(), true);
          senderOutput.println(Protocol.REMOVE_COMPLETE_TOKEN);
        }
      }

    } catch (Exception e) {
      System.out.println("Error receiving acknowledgement:  " + e);
    }
  }

  public static synchronized void handleJoin(int port, Socket client) {
    portsInUse.add(port);
    dataStoreList.put(port, client);
    reverseDataStoreList.put(client, port);
    if (!checkEnoughDataStores()) { new Thread(Controller::startRebalance).start(); }
  }

  public static synchronized void handleListClient (PrintWriter output){
    if (checkEnoughDataStores()) {
      output.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      return;
    }
    StringBuilder files = new StringBuilder();
    for(String file : index.getFiles()){
      if(index.getStatus(file).equals(Protocol.STORED)){
        files.append(" ").append(file);
      }
    }
    output.println(Protocol.LIST_TOKEN + files);
  }

  public static void handleListServer(String response, Socket client){
    if (response != null) {
      synchronized (AcknowledgementTracker.class) {
        int port = reverseDataStoreList.get(client);
        fileLists.put(port, response);
        latch.countDown();
      }
    }
  }

  public synchronized static void startRebalance(){
    try {
      scheduleNextRebalance();
      while(!checkEnoughDataStores()) {
        if (currentStorePorts.isEmpty() && currentRemovePorts.isEmpty()) {
          System.out.println("REBALANCE STARTED");
          sendListMessages();
          break;
        }
      }
    } catch (Exception e){
      System.out.println("Error starting rebalance: " + e);
    }
  }

  public static synchronized void sendListMessages(){
    try {
      latch = new CountDownLatch(dataStoreList.keySet().size());
      fileLists = new ConcurrentHashMap<>();

      for (int port : dataStoreList.keySet()) {
        Socket dataStore = dataStoreList.get(port);
        System.out.println(dataStore);
        PrintWriter dataStoreOutput = new PrintWriter(dataStore.getOutputStream(), true);
        dataStoreOutput.println(Protocol.LIST_TOKEN);
      }

      boolean listsReceived = latch.await(timeout, TimeUnit.MILLISECONDS);
      System.out.println(fileLists);
      if(fileLists.size() < replicationFactor){
        return;
      }
      HashMap<Integer, ArrayList<String>> parsedFileLists = new HashMap<>();
      for (Integer port : fileLists.keySet()) {
        parsedFileLists.put(port, parseFileList(fileLists.get(port)));
      }
      calculateRebalance(parsedFileLists);
    } catch (Exception e){
      System.out.println("Error sending list messages: " + e);
    }
  }

  public static synchronized ArrayList<String> parseFileList(String fileList){
    ArrayList<String> files = new ArrayList<>(Arrays.asList(fileList.split(" ")));
    files.remove(0);
    return files;
  }

  public static synchronized void calculateRebalance(HashMap<Integer, ArrayList<String>> fileLists){

    System.out.println(fileLists);

    newIndex = new Index();
    for (String file : index.getFiles()){
      newIndex.addValue(String.valueOf(file), String.valueOf(index.getStatus(file)), Integer.valueOf(index.getSize(file)), new ArrayList<>(index.getDataStores(file)));
    }

    ArrayList<String> fullFileList = new ArrayList<>();
    for (ArrayList<String> list : fileLists.values()){
      fullFileList.addAll(list);
    }

    filesToDelete = new HashMap<>();
    filesToSend = new HashMap<>();
    for (int port : fileLists.keySet()) {
      filesToDelete.put(port, new ArrayList<>());
      filesToSend.put(port, new HashMap<>());
    }

    // Remove files from index
    for (String file : index.getFiles()){
      if (!fullFileList.contains(file)){
        index.remove(file);
      }
    }

    // determine non indexed files
    ArrayList<String> nonIndexedFiles = new ArrayList<>();
    for (int port : fileLists.keySet()) {
      for (String file : fileLists.get(port)) {
        if (!index.contains(file)){
          nonIndexedFiles.add(file);
          ArrayList<String> temp = filesToDelete.getOrDefault(port, new ArrayList<>());
          temp.add(file);
          filesToDelete.put(port, temp);
        }
      }
    }

    for (String file : index.getFiles()) {
      if (index.getStatus(file).equals(Protocol.REMOVING)) {
        for (int port : fileLists.keySet()) {
          if (fileLists.get(port).contains(file)) {
            nonIndexedFiles.add(file);
            ArrayList<String> temp = filesToDelete.getOrDefault(port, new ArrayList<>());
            temp.add(file);
            filesToDelete.put(port, temp);
          }
        }
        index.remove(file);
      }
    }

    // Remove non indexed values
    for (ArrayList<String> fileList : fileLists.values()){
      for (String file : nonIndexedFiles) {
        fileList.remove(file);
      }
    }

    // Make sure index matches with returned lists
    HashMap<String, ArrayList<Integer>> tempIndex = new HashMap<>();
    for (int port : fileLists.keySet()){
      for (String file : fileLists.get(port)){
        ArrayList<Integer> tempDataStoreList = tempIndex.getOrDefault(file, new ArrayList<>());
        tempDataStoreList.add(port);
        tempIndex.put(file, tempDataStoreList);
      }
    }
    for(String file: tempIndex.keySet()){
      index.updateDataStores(file, tempIndex.get(file));
    }

    double minRebalanceValue = Math.floor((double) (replicationFactor * index.getFiles().size()) / fileLists.size());
    double maxRebalanceValue = Math.ceil((double) (replicationFactor * index.getFiles().size()) / fileLists.size());

    HashMap<String, ArrayList<Integer>> filesToAddToIndex = new HashMap<>();
    for (String file : index.getFiles()){
      filesToAddToIndex.put(file, new ArrayList<>());
    }

    while (minimumRebalanceCheck(fileLists, minRebalanceValue)){
      int minDataStore = getMinDataStore(fileLists);
      ArrayList<String> leastStoredFiles = getLeastStoredFiles(fileLists);

      int count = 0;
      String leastStoredFile = leastStoredFiles.get(count);
      while (fileLists.get(minDataStore).contains(leastStoredFile)) {
        count++;
        leastStoredFile = leastStoredFiles.get(count);
      }

      ArrayList<String> currentStoredFilesAtDataStore = fileLists.get(minDataStore);
      currentStoredFilesAtDataStore.add(leastStoredFile);
      fileLists.put(minDataStore, currentStoredFilesAtDataStore);

      count = 0;
      int dataStoreToSend = index.getDataStores(leastStoredFile).get(count);
      while (!fileLists.get(dataStoreToSend).contains(leastStoredFile)){
        count++;
        dataStoreToSend = index.getDataStores(leastStoredFile).get(count);
      }
      ArrayList<Integer> currentDataStoresToSend = filesToSend.get(dataStoreToSend).getOrDefault(leastStoredFile, new ArrayList<>());
      currentDataStoresToSend.add(minDataStore);
      HashMap<String, ArrayList<Integer>> fileToSend = filesToSend.get(dataStoreToSend);
      fileToSend.put(leastStoredFile, currentDataStoresToSend);
      filesToSend.put(dataStoreToSend, fileToSend);

      ArrayList<Integer> addToIndex = filesToAddToIndex.get(leastStoredFile);
      addToIndex.add(minDataStore);
      filesToAddToIndex.put(leastStoredFile, addToIndex);
    }

    for(String file : filesToAddToIndex.keySet()){
      for (int port : filesToAddToIndex.get(file)){
        index.addDataStore(file, port);
      }
    }

    while (maximumRebalanceCheck(fileLists, maxRebalanceValue)){
      int maxDataStore = getMaxDataStore(fileLists);
      ArrayList<String> mostStoredFiles = getMostStoredFiles(fileLists);

      int count = 0;
      String mostStoredFile = mostStoredFiles.get(count);
      while (!fileLists.get(maxDataStore).contains(mostStoredFile)) {
        count++;
        mostStoredFile = mostStoredFiles.get(count);
      }

      ArrayList<String> currentFilesToDelete = filesToDelete.getOrDefault(maxDataStore, new ArrayList<>());
      currentFilesToDelete.add(mostStoredFile);
      filesToDelete.put(maxDataStore, currentFilesToDelete);

      ArrayList<String> currentStoredFilesAtDataStore = fileLists.get(maxDataStore);
      currentStoredFilesAtDataStore.remove(mostStoredFile);
      fileLists.put(maxDataStore, currentStoredFilesAtDataStore);

      index.removeDataStore(mostStoredFile, maxDataStore);
    }

    System.out.println(filesToSend);
    System.out.println(filesToDelete);
    System.out.println(fileLists);

    parseSendAndDeleteLists(filesToSend, filesToDelete);
  }

  public static synchronized void parseSendAndDeleteLists(HashMap<Integer, HashMap<String, ArrayList<Integer>>> filesToSend, HashMap<Integer, ArrayList<String>> filesToDelete){

    HashMap<Integer, String> parsedFilesToSend = new HashMap<>();
    HashMap<Integer, String> parsedFilesToDelete = new HashMap<>();

    for (int port : filesToSend.keySet()){
      System.out.println(port);
      StringBuilder sendStringBuilder = new StringBuilder();
      System.out.println(filesToSend.get(port).size());
      sendStringBuilder.append(filesToSend.get(port).size());
      for (String file : filesToSend.get(port).keySet()){
        sendStringBuilder.append(" ").append(file);
        sendStringBuilder.append(" ").append(filesToSend.get(port).getOrDefault(file, new ArrayList<>()).size());
        for(int destination : filesToSend.get(port).getOrDefault(file, new ArrayList<>())){
          sendStringBuilder.append(" ").append(destination);
        }
      }
      parsedFilesToSend.put(port, String.valueOf(sendStringBuilder));
    }

    for (int port : filesToDelete.keySet()){
      StringBuilder deleteStringBuilder = new StringBuilder();
      deleteStringBuilder.append(filesToDelete.get(port).size());
      for(String file : filesToDelete.get(port)){
        deleteStringBuilder.append(" ").append(file);
      }
      parsedFilesToDelete.put(port, String.valueOf(deleteStringBuilder));
    }

    System.out.println("PARSED SEND AND DELETE");
    System.out.println(parsedFilesToSend);
    System.out.println(parsedFilesToDelete);
    System.out.println("PARSED MESSAGES");

    HashMap<Integer, String> rebalanceMessages = new HashMap<>();
    for (Integer port : parsedFilesToSend.keySet()) {
      String rebalanceMessage = parsedFilesToSend.get(port) + " " + parsedFilesToDelete.get(port);
      System.out.println(rebalanceMessage);
      rebalanceMessages.put(port, rebalanceMessage);
    }
    sendRebalanceMessages(rebalanceMessages);
  }

  public static synchronized void sendRebalanceMessages(HashMap<Integer, String> rebalanceMessages) {
    try {
      latch2 = new CountDownLatch(rebalanceMessages.size());

      for (int port : rebalanceMessages.keySet()){
        Socket dataStore = dataStoreList.get(port);
        PrintWriter dataStoreOutput = new PrintWriter(dataStore.getOutputStream(), true);
        BufferedReader dataStoreInput = new BufferedReader(new InputStreamReader(dataStore.getInputStream()));
        dataStoreOutput.println(Protocol.REBALANCE_TOKEN + " " + rebalanceMessages.get(port));
      }
      boolean rebalanceAcksReceived = latch2.await(timeout, TimeUnit.MILLISECONDS);
      //index = newIndex; // This might be nescaserry
      System.out.println("rebalance successful? " + rebalanceAcksReceived);
    } catch (Exception e){
      System.out.println("Error sending rebalance messages: " + e);
    }
  }

  public static void handleRebalanceComplete(Socket client){
    synchronized (AcknowledgementTracker.class){
      int port = reverseDataStoreList.get(client);
      HashMap<String, ArrayList<Integer>> addedFiles = filesToSend.get(port);
      for (String file : addedFiles.keySet()){
        for (int addedPort : addedFiles.get(file)){
          newIndex.addDataStore(file, addedPort);
        }
      }
      ArrayList<String> deletedFiles = filesToDelete.get(port);
      for (String file : deletedFiles) {
        newIndex.removeDataStore(file, port);
      }
      latch2.countDown();
    }
  }

  public static synchronized ArrayList<String> getLeastStoredFiles (HashMap<Integer, ArrayList<String>> fileLists){
    ArrayList<String> allFiles = new ArrayList<>();
    for(ArrayList<String> fileList : fileLists.values()){
      allFiles.addAll(fileList);
    }
    HashMap<String, Integer> fileCount = new HashMap<>();
    for (String file : allFiles) {
      fileCount.put(file, fileCount.getOrDefault(file, 0) + 1);
    }

    ArrayList<HashMap.Entry<String, Integer>> sorted = new ArrayList<>(fileCount.entrySet());
    sorted.sort(Comparator.comparingInt(Map.Entry::getValue));

    ArrayList<String> leastStored = new ArrayList<>();
    for (Map.Entry<String, Integer> entry : sorted) {
      leastStored.add(entry.getKey());
    }
    return leastStored;
  }

  public static synchronized ArrayList<String> getMostStoredFiles (HashMap<Integer, ArrayList<String>> fileLists){
    ArrayList<String> mostStoredFiles = getLeastStoredFiles(fileLists);
    Collections.reverse(mostStoredFiles);
    return mostStoredFiles;
  }

  public static synchronized int getMinDataStore(HashMap<Integer, ArrayList<String>> fileLists){
    int minDataStore = 0;
    int minNumFiles = Integer.MAX_VALUE;
    for (int port : fileLists.keySet()){
      if (fileLists.get(port).size() < minNumFiles){
        minNumFiles = fileLists.get(port).size();
        minDataStore = port;
      }
    }
    return minDataStore;
  }

  public static synchronized int getMaxDataStore(HashMap<Integer, ArrayList<String>> fileLists){
    int minDataStore = 0;
    int minNumFiles = Integer.MIN_VALUE;
    for (int port : fileLists.keySet()){
      if (fileLists.get(port).size() > minNumFiles){
        minNumFiles = fileLists.get(port).size();
        minDataStore = port;
      }
    }
    return minDataStore;
  }

  public static synchronized boolean minimumRebalanceCheck(HashMap<Integer, ArrayList<String>> fileLists, double minRebalanceValue){
    ArrayList<String> files = index.getFiles();
    HashMap<String, Integer> fileNumbers = new HashMap<>();
    for (ArrayList<String> fileList : fileLists.values()){
      if (fileList.size() < minRebalanceValue) {
        return true;
      }
      for (String file : files) {
        if (fileList.contains(file)){
          fileNumbers.put(file, fileNumbers.getOrDefault(file, 0) + 1);
        }
      }
    }
    for (int numFiles : fileNumbers.values()){
      if (numFiles < replicationFactor) {
        return true;
      }
    }
    return false;
  }

  public static synchronized boolean maximumRebalanceCheck(HashMap<Integer, ArrayList<String>> fileLists, double maxRebalanceValue){
    ArrayList<String> files = index.getFiles();
    HashMap<String, Integer> fileNumbers = new HashMap<>();
    for (ArrayList<String> fileList : fileLists.values()){
      if (fileList.size() > maxRebalanceValue) {
        return true;
      }
      for (String file : files) {
        if (fileList.contains(file)){
          fileNumbers.put(file, fileNumbers.getOrDefault(file, 0) + 1);
        }
      }
    }
    for (int numFiles : fileNumbers.values()){
      if (numFiles > replicationFactor) {
        return true;
      }
    }
    return false;
  }

  public static synchronized boolean checkEnoughDataStores(){
    return portsInUse.size() < replicationFactor;
  }


  public static synchronized ArrayList<Integer> getLeastUsedStore(){
    ArrayList<Integer> allDataStores = new ArrayList<>();
    for(ArrayList<Integer> dataStorePorts : index.fileDataStores.values()){
      allDataStores.addAll(dataStorePorts);
    }

    ArrayList<Integer> unusedDataStores = new ArrayList<>();
    for (Integer dataStore : portsInUse){
      if (!allDataStores.contains(dataStore)){
        unusedDataStores.add(dataStore);
      }
    }

    HashMap<Integer, Integer> dataStorePortCount = new HashMap<>();
    for (Integer port : allDataStores) {
      dataStorePortCount.put(port, dataStorePortCount.getOrDefault(port, 0) + 1);
    }

    ArrayList<HashMap.Entry<Integer, Integer>> sorted = new ArrayList<>(dataStorePortCount.entrySet());
    sorted.sort(Comparator.comparingInt(Map.Entry::getValue));

    ArrayList<Integer> leastUsed = new ArrayList<>();
    for (Map.Entry<Integer, Integer> entry : sorted) {
      leastUsed.add(entry.getKey());
    }

    unusedDataStores.addAll(leastUsed);

    return unusedDataStores;
  }
}
