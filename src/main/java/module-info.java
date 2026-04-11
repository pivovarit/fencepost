module com.pivovarit.fencepost {
    exports com.pivovarit.fencepost;
    exports com.pivovarit.fencepost.function;
    exports com.pivovarit.fencepost.lock;
    exports com.pivovarit.fencepost.queue;

    requires java.sql;
    requires java.naming;
    requires org.slf4j;
    requires jdk.httpserver;
}
