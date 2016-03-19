package gr.tuc.softnet.engine;

import gr.tuc.softnet.core.MCInitializable;

/**
 * Created by vagvaz on 10/02/16.
 */
public interface MCNode extends IDable {
    void waitForExit();
    void kill();

}
