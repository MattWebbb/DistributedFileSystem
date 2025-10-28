import java.io.IOException;

public class FileDoesNotExistException extends IOException {
   private static final long serialVersionUID = 3234056968693564846L;

   public FileDoesNotExistException(String filename) {
      super("Error trying to load or remove file " + filename + " - file does not exist");
   }
}
