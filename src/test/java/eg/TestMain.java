package eg;

import com.sun.tools.internal.xjc.generator.bean.BeanGenerator;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;

/**
 * Created by Rob on 20/12/14.
 */
public class TestMain {

    public static void main(String... args) throws IntrospectionException {
        BeanInfo info= Introspector.getBeanInfo(TestBeanClass .class);
        System.out.println(info);

        info.getPropertyDescriptors();
    }
}
