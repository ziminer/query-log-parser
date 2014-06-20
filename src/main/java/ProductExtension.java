import com.xhive.core.interfaces.XhiveSessionIf;
import com.xhive.dom.interfaces.XhiveLibraryChildIf;
import com.xhive.error.xquery.XhiveXQueryErrorException;
import com.xhive.query.interfaces.XhiveXQueryExtensionFunctionIf;
import com.xhive.query.interfaces.XhiveXQueryValueIf;
import org.w3c.dom.Node;

import java.util.Iterator;

public class ProductExtension implements XhiveXQueryExtensionFunctionIf {
   private XhiveSessionIf _session;

   public ProductExtension(XhiveSessionIf session) {
      _session = session;
   }

   public Object[] call(Iterator<? extends XhiveXQueryValueIf>[] iterators) {
      if (iterators.length != 1) {
         throw new XhiveXQueryErrorException("extension: bad number of arguments: expect: 1, got: " +
               iterators.length);
      }
      String product = iterators[0].next().asString();
      // path validation
      if (product.contains("/") || product.contains("..")) {
         throw new XhiveXQueryErrorException("Invalid product name: " + product);
      }
      XhiveLibraryChildIf child = _session.getDatabase().getRoot().get(product);
      if (child == null) {
         return new Node[0];
      } else {
         return new Node[] { child };
      }
   }
}
