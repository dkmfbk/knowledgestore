package eu.fbk.knowledgestore.distribution;

import eu.fbk.knowledgestore.runtime.Launcher;

public class StoreTester {

    public static void main(final String... args) {
        // when running, add src/main/config to classpath
        System.setProperty("launcher.executable", "ksd");
        System.setProperty("launcher.description", "Runs the KnowledgeStore daemon, "
                + "using the daemon and logging configurations optionally specified");
        System.setProperty("launcher.config", "/Users/alessio/Desktop/ks/ks.ttl");
        Launcher.main(args);
    }

}
