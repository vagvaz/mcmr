package gr.tuc.softnet.engine;

import gr.tuc.softnet.core.ConfStringConstants;
import org.apache.commons.configuration.HierarchicalConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by vagvaz on 03/03/16.
 */
public class JobConfiguration extends HierarchicalConfiguration {
    public JobConfiguration(){
        super();
    }

    public List<String> getMicroclouds() {
        List<Object> configList =  getList(ConfStringConstants.CLOUD_LIST,null);
        if(configList != null){
            List<String> result = new ArrayList<>();
            for(Object microCloud : configList){
                result.add(microCloud.toString());
                return result;
            }
        }
        return null;
    }
}
