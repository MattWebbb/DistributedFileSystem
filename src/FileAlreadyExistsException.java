import java.io.IOException;

public class FileAlreadyExistsException extends IOException {
   private static final long serialVersionUID = -2050950133394985954L;

   public FileAlreadyExistsException(String filename) {
      super("Error trying to store file " + filename + " - file already exists");
   }
}
