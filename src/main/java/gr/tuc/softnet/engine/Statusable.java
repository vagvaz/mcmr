package gr.tuc.softnet.engine;

import org.apache.commons.configuration.Configuration;

/**
 * Created by vagvaz on 10/02/16.
 */
public interface Statusable {
    String getID();
    Configuration getStatus();
}
