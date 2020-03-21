package com.danial.jmx;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.text.SimpleDateFormat;
import java.util.HashMap;

public class JMXClient {

    /**
     *
     * @param args
     *            host port username password bean name method ...params
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        if ((args.length < 6) || (args.length % 2 != 0))
        {
            logErr("params error");
            return;
        }
        final String host = args[0];

        final int rmiPort = Integer.valueOf(args[1]).intValue();

        final String username = args[2];

        final String password = args[3];

        final ObjectName objectName = new ObjectName(args[4]);

        final String methodName = args[5];

        final HashMap<String, String[]> jmxParamsHashMap = new HashMap<String, String[]>();

        final String[] usernameAndPassword =
                { username, password };

        jmxParamsHashMap.put("jmx.remote.credentials", usernameAndPassword);
        final String serviceUrl = new StringBuilder().append("service:jmx:rmi:///jndi/rmi://").append(host).append(":")
                .append(rmiPort).append("/jmxrmi").toString();
        final JMXServiceURL jmxServiceURL = new JMXServiceURL(serviceUrl);

        final JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxServiceURL, jmxParamsHashMap);
        if (jmxConnector == null)
        {
            logErr(new StringBuilder().append("connect to jmx failed, url=").append(jmxServiceURL).toString());
            return;
        }

        log(new StringBuilder().append("JMXConnector=").append(jmxConnector.toString()).toString());

        Object[] paramsValue = null;
        String[] paramsClassName = null;

        final MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();

        final Object localObject = mBeanServerConnection.invoke(objectName, methodName, paramsValue, paramsClassName);

        log(new StringBuilder().append("invoke method success, name=").append(objectName).append(", operation=")
                .append(methodName).append(", retvalue=").append(localObject == null ? "void" : localObject.toString())
                .toString());
    }

    static void log(String paramString)
    {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss : ");
        final StringBuilder sBuilder = new StringBuilder();
        sBuilder.append(simpleDateFormat.format(Long.valueOf(System.currentTimeMillis())));
        sBuilder.append(paramString);

        System.out.println(sBuilder.toString());
    }

    static void logErr(String paramString)
    {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss : ");
        final StringBuilder sBuilder = new StringBuilder();
        sBuilder.append(simpleDateFormat.format(Long.valueOf(System.currentTimeMillis())));
        sBuilder.append(paramString);

        System.err.println(sBuilder.toString());
    }
}
