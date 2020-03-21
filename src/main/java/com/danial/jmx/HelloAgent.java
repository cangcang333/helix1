package com.danial.jmx;

import com.sun.jdmk.comm.HtmlAdaptorServer;

import javax.management.*;
import java.lang.management.ManagementFactory;

public class HelloAgent {
    public static void main(String[] args) throws MalformedObjectNameException, InterruptedException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName helloName = new ObjectName("jmxBean:name=hello");
        // create mbean and register mbean
        server.registerMBean(new Hello(), helloName);

        ObjectName adaptorName = new ObjectName("HelloAgent:name=htmladaptor,port=8082");
        HtmlAdaptorServer adaptorServer = new HtmlAdaptorServer();
        server.registerMBean(adaptorServer, adaptorName);
        adaptorServer.start();

        Thread.sleep(60 * 60 * 1000);
    }
}
