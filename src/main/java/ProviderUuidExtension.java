import com.xhive.dom.interfaces.XhiveLibraryIf;
import com.xhive.dom.interfaces.XhiveNodeIf;
import com.xhive.error.xquery.XhiveXQueryErrorException;
import com.xhive.query.interfaces.XhiveXQueryExtensionFunctionIf;
import com.xhive.query.interfaces.XhiveXQueryValueIf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ProviderUuidExtension implements XhiveXQueryExtensionFunctionIf {

   @Override
   public Object[] call(Iterator<? extends XhiveXQueryValueIf>[] iterators) {
      if (iterators.length != 1) {
         throw new XhiveXQueryErrorException(
               "extension: bad number of arguments; expect: 1, got: "
                     + iterators.length);
      }

      List<String> result = new ArrayList<String>();
      for (Iterator<? extends XhiveXQueryValueIf> nodeIterator = iterators[0];
           nodeIterator.hasNext(); ) {

         // fetch document
         XhiveXQueryValueIf val = nodeIterator.next();

         if (!val.isNode()) {
            throw new XhiveXQueryErrorException(
                  "extension: value not node: right type:" + val.getNodeType());
         }
         XhiveNodeIf node = val.asNode();
         XhiveLibraryIf lib = node.getOwnerDocument().getOwnerLibrary();

         if (lib == null) {
            throw new XhiveXQueryErrorException(
                  "unable to locate owner lib for document");
         }

         result.add(lib.getName());
      }
      return result.toArray(new String[result.size()]);
   }
}
