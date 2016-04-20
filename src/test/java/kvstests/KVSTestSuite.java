package kvstests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Created by ap0n on 5/4/2016.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    PipelineSingleTest.class,
    PipelineMultiTest.class,
    KVSSingeTest.class,
    KVSMultiTest.class
})
public class KVSTestSuite { }
