import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class Index {

  ConcurrentHashMap<String,String> index;
  ConcurrentHashMap<String,Integer> fileSizeMap;
  ConcurrentHashMap<String, ArrayList<Integer>> fileDataStores;

  public Index(){
    this.index = new ConcurrentHashMap<>();
    this.fileSizeMap = new ConcurrentHashMap<>();
    this.fileDataStores = new ConcurrentHashMap<>();
  }

  public synchronized void addValue (String name, String status, int fileSize, ArrayList<Integer> dataStores){
    index.put(name, status);
    fileSizeMap.put(name, fileSize);
    fileDataStores.put(name, dataStores);
  }

  public synchronized ArrayList<String> getFiles(){
    return new ArrayList<>(index.keySet());
  }

  public synchronized ConcurrentHashMap<String, String> getStatusList() { return index; }

  public synchronized String getStatus (String name){
    return index.get(name);
  }

  public synchronized int getSize (String name){
    return fileSizeMap.get(name);
  }

  public synchronized ArrayList<Integer> getDataStores (String name) {
    return new ArrayList<>(fileDataStores.get(name));
  }


  public synchronized void updateStatus (String name, String status){
    if(index.containsKey(name)){
      index.put(name, status);
    }
  }

  public synchronized void updateDataStores (String name, ArrayList<Integer> dataStores){
    if(fileDataStores.containsKey(name)){
      fileDataStores.put(name, dataStores);
    }
  }

  public synchronized void addDataStore(String name, int port){
    ArrayList<Integer> updatedDataStores = fileDataStores.get(name);
    updatedDataStores.add(port);
    fileDataStores.put(name, updatedDataStores);
  }

  public synchronized void removeDataStore(String name, int port){
    ArrayList<Integer> updatedDataStores = fileDataStores.get(name);
    updatedDataStores.remove(Integer.valueOf(port));
    fileDataStores.put(name, updatedDataStores);
  }

  public synchronized void remove (String name){
    index.remove(name);
    fileSizeMap.remove(name);
    fileDataStores.remove(name);
  }

  public synchronized void removePort (int port){
    for(ArrayList<Integer> dataStorePorts : fileDataStores.values()){
      dataStorePorts.remove(Integer.valueOf(port));
    }
  }

  public synchronized void clear(){
    index.clear();
    fileSizeMap.clear();
    fileDataStores.clear();
  }

  public synchronized boolean contains(String name){
    return index.containsKey(name);
  }
}
