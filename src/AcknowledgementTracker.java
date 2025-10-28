import java.net.Socket;
import java.util.ArrayList;

public class AcknowledgementTracker {

  private Socket sender;
  private ArrayList<Integer> ports;

  public AcknowledgementTracker(Socket sender, ArrayList<Integer> ports){
    this.sender = sender;
    this.ports = ports;
  }

  public synchronized Socket getSender() {
    return sender;
  }

  public synchronized ArrayList<Integer> getPorts() {
    return ports;
  }

  public synchronized void setPorts(ArrayList<Integer> ports) {
    this.ports = ports;
  }
}
