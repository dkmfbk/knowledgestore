package eu.fbk.knowledgestore.populator.naf;

import java.io.IOException;

public class finalizeThread implements Runnable {

    @Override
    public void run() {
        while (true) {
            if (allThreadsDied()) {
                try {
                    footer();
                    return;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
           
        }
    }

    private boolean allThreadsDied() {
        boolean notFinished = true;
        for (Thread tmp : nafPopulator.threads) {
            if (tmp.isAlive())
                notFinished = false;
            //System.err.println("Threads(" + tmp.getId() + ") - alive:" + tmp.isAlive());
        }

       // System.err.println("Threads alive check: " + notFinished);
        return notFinished;
    }

    void footer() throws IOException {

        nafPopulator.out.append("Global stats:\n").append(nafPopulator.globalStats.getStats());
        nafPopulator.out.flush();

        if (!nafPopulator.printToFile && (nafPopulator.JobFinished)) {
            nafPopulator.closeConnection();
        } else {
            nafPopulator.mentionFile.flush();
            nafPopulator.mentionFile.close();
        }
        // out.append("Global stats:\n").append(globalStats.getStats());
        // out.flush();
        nafPopulator.out.close();
        nafPopulator.nullObjects();
    }

}
