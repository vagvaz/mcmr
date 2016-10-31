package gr.tuc.softnet;
import java.util.Iterator;
/**
 * Created by vagvaz on 24/08/16.
 */
public class Utils {

    public static <E> Iterable<E> iterable(final Iterator<E> iterator) {
      if (iterator == null) {
        throw new NullPointerException();
      }
      return new Iterable<E>() {
        public Iterator<E> iterator() {
          return iterator;
        }
      };
    }

}
