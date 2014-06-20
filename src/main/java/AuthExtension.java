import com.xhive.query.interfaces.XhiveXQueryExtensionFunctionIf;
import com.xhive.query.interfaces.XhiveXQueryValueIf;

import java.util.Iterator;

public class AuthExtension implements XhiveXQueryExtensionFunctionIf {
   @Override
   public Object[] call(Iterator<? extends XhiveXQueryValueIf>[] iterators) {
      return new Object[] {true};
   }
}
