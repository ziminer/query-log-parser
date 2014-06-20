import com.xhive.error.xquery.XhiveXQueryErrorException;
import com.xhive.query.interfaces.XhiveXQueryExtensionFunctionIf;
import com.xhive.query.interfaces.XhiveXQueryValueIf;

import java.util.Iterator;
import java.util.UUID;

/**
 * Extension function to return store UUID. Fake!
 */
public class StoreUuidExtension implements XhiveXQueryExtensionFunctionIf {

   UUID _uuid = UUID.randomUUID();

   @Override
   public Object[] call(Iterator<? extends XhiveXQueryValueIf>[] iterators) {
      if (iterators.length != 0) {
         throw new XhiveXQueryErrorException("extension: bad number of arguments; expect: 0, got: " + iterators.length);
      }
      return new Object[] {
            _uuid.toString()
      };
   }
}
